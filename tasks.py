import os
from celery import Celery
from celery.utils.log import get_task_logger
import assemblyai as aai
import boto3
import psycopg2

app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)

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
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

@app.task
def process_transcription(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Fetch the job details
        cursor.execute("SELECT s3_audio_key FROM transcription_jobs WHERE id = %s AND status = 'pending'", (job_id,))
        job = cursor.fetchone()

        if not job:
            logger.info(f"No pending job found for job_id {job_id}")
            return

        s3_audio_key = job[0]

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

