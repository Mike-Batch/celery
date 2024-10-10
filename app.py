import os
from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks import process_transcription
import psycopg2

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

@app.route('/')
def main():
    return render_template('main.html')

@app.route('/transcribe', methods=['POST'])
def transcribe():
    s3_audio_key = request.form.get('s3_audio_key')
    if not s3_audio_key:
        flash("Audio file path is required.")
        return redirect('/')

    # Insert job into the database
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

        # Add the transcription job to the Celery queue
        process_transcription.delay(job_id)

        flash(f"Your transcription job (ID: {job_id}) has been submitted.")
    except Exception as e:
        flash(f"Error submitting job: {str(e)}")

    return redirect('/')

# Endpoint to check the status of a transcription job
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
