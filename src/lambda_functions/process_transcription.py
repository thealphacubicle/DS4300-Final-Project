import os
import json
import boto3
import pymysql
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize sentiment analyzer.
analyzer = SentimentIntensityAnalyzer()

# Retrieve database connection details from environment variables.
DB_HOST = os.environ['DB_HOST']  # e.g., "mydbinstance.abcdefghijk.us-east-2.rds.amazonaws.com"
DB_USER = os.environ['DB_USER']  # e.g., "admin"
DB_PASSWORD = os.environ['DB_PASSWORD']  # e.g., "yourpassword"
DB_NAME = os.environ['DB_NAME']  # e.g., "transcriptionsdb"


def lambda_handler(event, context):
    try:
        # Extract bucket and object key from the S3 event.
        record = event['Records'][0]
        s3_info = record['s3']
        bucket = s3_info['bucket']['name']
        key = s3_info['object']['key']
        print(f"Processing JSON file from bucket: {bucket}, key: {key}")

        # Retrieve the JSON object from S3 using the boto3 client.
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        transcript_json = json.loads(response['Body'].read().decode('utf-8'))
        print("Retrieved JSON object from S3")

        # Extract the transcription text from the JSON.
        transcripts = transcript_json.get("results", {}).get("transcripts", [])
        if not transcripts or "transcript" not in transcripts[0]:
            raise Exception("No transcription text found in the result.")
        transcription_text = transcripts[0]["transcript"]
        print("Extracted transcription text:", transcription_text)

        # Perform sentiment analysis using VADER.
        sentiment_scores = analyzer.polarity_scores(transcription_text)
        sentiment_score = sentiment_scores.get("compound")
        print("Computed sentiment score:", sentiment_score)

        # Extract file details from the key.
        audio_file_name = os.path.basename(key)
        file_type = audio_file_name.split('.')[-1].lower()

        # Connect to the RDS MySQL Database using PyMySQL.
        connection = None
        try:
            connection = pymysql.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
            )
            print("Connected to RDS MySQL database.")

            with connection.cursor() as cursor:
                # Create the table if it doesn't exist.
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS transcriptions (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        audio_file_name VARCHAR(255),
                        file_type VARCHAR(50),
                        transcription_text TEXT,
                        transcription_sentiment_score FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                cursor.execute(create_table_query)
                print("Ensured table exists.")

                # Insert transcription record.
                insert_query = """
                    INSERT INTO transcriptions (audio_file_name, file_type, transcription_text, transcription_sentiment_score)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (audio_file_name, file_type, transcription_text, sentiment_score))
                connection.commit()
                print("Record inserted successfully into RDS.")

        except Exception as db_err:
            print("Database error:", db_err)
            raise db_err

        finally:
            if connection:
                connection.close()
                print("Database connection closed.")

        # Return a successful response.
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Transcription processed and record inserted into RDS successfully.",
                "audio_file_name": audio_file_name,
                "sentiment_score": sentiment_score
            })
        }

    except Exception as e:
        print("Error during processing:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error processing transcription.",
                "error": str(e)
            })
        }