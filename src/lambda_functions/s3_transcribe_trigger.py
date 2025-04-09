import boto3
import os
import uuid

# Initialize the Transcribe client using the region from environment variables
client = boto3.client('transcribe')


def lambda_handler(event, context):
    try:
        # Assuming one record per event - extract S3 bucket and object key
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # Construct the S3 URI for the media file
        media_uri = f"s3://{bucket}/{object_key}"

        # Generate a unique job name using a UUID
        job_name = f"TranscriptionJob-{str(uuid.uuid4())}"
        file_type = object_key.split(".")[-1].lower()

        # Start the Transcription Job based on file type
        if file_type == "mp3":
            response = client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': media_uri},
                MediaFormat="mp3",
                LanguageCode=os.getenv("TRANSCRIBE_LANGUAGE_CODE", "en-US")
            )
        elif file_type == "wav":
            response = client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': media_uri},
                MediaFormat="wav",
                LanguageCode=os.getenv("TRANSCRIBE_LANGUAGE_CODE", "en-US")
            )
        else:
            raise ValueError(f"Unsupported file type: {file_type}. Pipeline only supports MP3 and WAV files!")

        return {
            'statusCode': 200,
            'TranscriptionJobName': response['TranscriptionJob']['TranscriptionJobName'],
        }

    except Exception as e:
        print("Error starting transcription job:", str(e))
        raise e
