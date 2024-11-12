import os
from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
import assemblyai as aai
import boto3
import psycopg2
from neo4j import GraphDatabase

app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)

# Configure Celery Beat to use the schedule
app.conf.beat_schedule = {
    'poll-transcription-jobs-every-1-mins': {
        'task': 'tasks.check_for_new_jobs',
        'schedule': crontab(minute='*/1'),  # Runs every 1 minutes
    },
    # Schedule the location sync task to run periodically, e.g., every 10 minutes
    'sync-location-records-every-1-mins': {
        'task': 'tasks.sync_location_records',
        'schedule': crontab(hour='*/3'),  # Runs every 3# hours
    },
}

# AssemblyAI setup
aai.settings.api_key = os.getenv("ASSEMBLYAI_API_KEY")

# S3 setup
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Database connection
def get_db_connection():
    db_url = os.getenv("DB_URL")
    return psycopg2.connect(db_url)


def get_neo4j_connection():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


# New task to sync location records
import decimal

@app.task
def sync_location_records(batch_size=10):
    # Establish database connections
    conn = get_db_connection()
    driver = get_neo4j_connection()
    cursor = conn.cursor()

    try:
        # Fetch a batch of unsynced locations with all required fields
        cursor.execute("""
            SELECT id, clientid, name, country, city, lat, long, functions, headcount, revenue, costs, margin 
            FROM locations 
            WHERE synced = FALSE 
            LIMIT %s
        """, (batch_size,))
        unsynced_locations = cursor.fetchall()

        if not unsynced_locations:
            logger.info("No unsynced locations found.")
            return

        # Open Neo4j session
        with driver.session() as session:
            for location in unsynced_locations:
                location_id, clientid, name, country, city, lat, long, functions, headcount, revenue, costs, margin = location

                # Convert any decimal.Decimal values to float or int as needed
                lat = float(lat) if isinstance(lat, decimal.Decimal) else lat
                long = float(long) if isinstance(long, decimal.Decimal) else long
                headcount = int(headcount) if isinstance(headcount, decimal.Decimal) else headcount
                revenue = float(revenue) if isinstance(revenue, decimal.Decimal) else revenue
                costs = float(costs) if isinstance(costs, decimal.Decimal) else costs
                margin = float(margin) if isinstance(margin, decimal.Decimal) else margin

                try:
                    # Begin Neo4j transaction for each location
                    with session.begin_transaction() as tx:
                        # Create Location node with PGClient_ID and other properties (Neo4j auto-generates `id`)
                        tx.run(
                            """
                            CREATE (l:Location {
                                PGClient_ID: $clientid,
                                name: $name,
                                country: $country,
                                city: $city,
                                lat: $lat,
                                long: $long,
                                functions: $functions,
                                headcount: $headcount,
                                revenue: $revenue,
                                costs: $costs,
                                margin: $margin
                            })
                            """,
                            clientid=clientid,
                            name=name,
                            country=country,
                            city=city,
                            lat=lat,
                            long=long,
                            functions=functions,
                            headcount=headcount,
                            revenue=revenue,
                            costs=costs,
                            margin=margin
                        )
                        logger.info(f"Location {name} created in Neo4j")

                        # Create HAS_LOCATION relationship from Company node to Location node
                        tx.run(
                            """
                            MATCH (c:Company {PGClient_ID: $clientid})
                            MATCH (l:Location {PGClient_ID: $clientid, name: $name})
                            CREATE (c)-[:HAS_LOCATION]->(l)
                            """,
                            clientid=clientid,
                            name=name
                        )
                        logger.info(f"Created HAS_LOCATION relationship from Company {clientid} to Location")

                    # Update the Postgres record to mark as synced
                    cursor.execute("UPDATE locations SET synced = TRUE WHERE id = %s", (location_id,))
                    conn.commit()

                except Exception as e:
                    # Log failure for this specific location without halting the batch process
                    logger.error(f"Failed to sync location {name} (ID: {location_id}): {e}")
                    conn.rollback()  # Rollback in case of an error with Postgres

    except Exception as e:
        logger.error(f"Error during sync_location_records execution: {e}")
    finally:
        # Close connections
        cursor.close()
        conn.close()
        driver.close()



# Periodic task to poll for new jobs
@app.task
def check_for_new_jobs():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Fetch pending jobs
        cursor.execute("SELECT id FROM transcription_jobs WHERE status = 'pending'")
        pending_jobs = cursor.fetchall()

        for job_id, in pending_jobs:
            # Trigger the transcription task for each pending job
            process_transcription.delay(job_id)
            logger.info(f"Triggered transcription task for job_id {job_id}")

    except Exception as e:
        logger.error(f"Error while polling for new jobs: {e}")

    finally:
        cursor.close()
        conn.close()

# Task to process a transcription job
@app.task
def process_transcription(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Fetch the job details, including fileid
        cursor.execute("SELECT s3_audio_key, fileid FROM transcription_jobs WHERE id = %s AND status = 'pending'", (job_id,))
        job = cursor.fetchone()

        if not job:
            logger.info(f"No pending job found for job_id {job_id}")
            return

        s3_audio_key, fileid = job

        # Update the job status to 'in_progress'
        cursor.execute("UPDATE transcription_jobs SET status = 'in_progress' WHERE id = %s", (job_id,))
        conn.commit()

        # Generate presigned URL for the audio file
        audio_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': os.getenv("S3_BUCKET_NAME"), 'Key': s3_audio_key},
            ExpiresIn=3600
        )

        # Perform transcription using AssemblyAI
        config = aai.TranscriptionConfig(speaker_labels=True)
        transcript = aai.Transcriber().transcribe(audio_url, config)

        # Create the transcription text
        transcription_text = "\n".join(
            [f"Speaker {utterance.speaker}: {utterance.text}" for utterance in transcript.utterances]
        )

        # Save the transcription to a TXT file
        local_txt_file = "/tmp/transcription.txt"
        with open(local_txt_file, "w") as f:
            f.write(transcription_text)

        # Upload the TXT file to S3
        s3_output_key = s3_audio_key.replace('.m4a', '.txt')
        s3_client.upload_file(local_txt_file, os.getenv("S3_BUCKET_NAME"), s3_output_key)

        # Update job status to 'completed'
        cursor.execute("UPDATE transcription_jobs SET status = 'completed', updated_at = NOW() WHERE id = %s", (job_id,))
        conn.commit()

        # Update the processing status in file_records to 'Transcribed'
        cursor.execute("UPDATE file_records SET processing = 'Transcribed' WHERE id = %s", (fileid,))
        conn.commit()

        # Insert a new record in file_records with updated filepath for the transcribed file
        cursor.execute("""
            INSERT INTO file_records (clientid, programid, type, filepath, shorttext, description, personid, audienceid, interviewer, purpose, date, processing, sourceid)
            SELECT clientid, programid, type, %s, shorttext, description, personid, audienceid, interviewer, purpose, date, 'Awaiting_POV_Processing', sourceid
            FROM file_records WHERE id = %s
        """, (s3_output_key, fileid))
        conn.commit()

        logger.info(f"Job {job_id} completed successfully, transcript saved to S3 at {s3_output_key}")

    except Exception as e:
        # Update job status to 'failed' and log the error message
        cursor.execute("UPDATE transcription_jobs SET status = 'failed', error_message = %s, updated_at = NOW() WHERE id = %s", (str(e), job_id))
        conn.commit()
        logger.error(f"Job {job_id} failed: {e}")

    finally:
        cursor.close()
        conn.close()
        # Clean up local TXT file
        if os.path.exists(local_txt_file):
            os.remove(local_txt_file)

