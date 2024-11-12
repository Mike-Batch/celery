import os
from flask import Flask, flash, redirect, request, jsonify
from tasks import process_transcription
import psycopg2
from neo4j import GraphDatabase

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

# Database connection
def get_db_connection():
    db_url = os.getenv("DB_URL")
    return psycopg2.connect(db_url)
    #     dbname=os.getenv("DB_NAME"),
    #     user=os.getenv("DB_USER"),
    #     password=os.getenv("DB_PASSWORD"),
    #     host=os.getenv("DB_HOST"),
    #     port=os.getenv("DB_PORT")
    # )

def get_neo4j_connection():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


@app.route('/transcribe', methods=['POST'])
def transcribe():
    s3_audio_key = request.form.get('s3_audio_key')
    if not s3_audio_key:
        flash("Audio file path is required.")
        return redirect('/')

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO transcription_jobs (s3_audio_key, status, created_at)
            VALUES (%s, %s, NOW()) RETURNING id
            """,
            (s3_audio_key, 'pending')
        )
        job_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        process_transcription.delay(job_id)

        return jsonify({'message': f'Your transcription job (ID: {job_id}) has been submitted.'})

    except Exception as e:
        return jsonify({'error': f'Error submitting job: {str(e)}'}), 500

@app.route('/status/<int:job_id>', methods=['GET'])
def get_status(job_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM transcription_jobs WHERE id = %s", (job_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return jsonify({"job_id": job_id, "status": result[0]})
        else:
            return jsonify({"error": "Job not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run()
