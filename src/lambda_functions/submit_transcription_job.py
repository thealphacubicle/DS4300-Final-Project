import os
import json
import uuid
import boto3

transcribe_client = boto3.client('transcribe')

# Use this environment variable for the intermediate S3 bucket name.
OUTPUT_BUCKET = os.environ.get("TRANSCRIBE_OUTPUT_BUCKET")


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        bucket = event.get("bucket")
        key = event.get("key")
        if not bucket or not key:
            raise ValueError("Missing required S3 bucket or key.")

        job_name = f"transcription-{uuid.uuid4()}"
        media_file_uri = f"s3://{bucket}/{key}"
        media_format = key.split('.')[-1].lower()
        language_code = os.environ.get("LANGUAGE_CODE", "en-US")

        print(f"Submitting transcription job: {job_name} for file: {media_file_uri}")

        # Prepare job parameters including the output bucket.
        job_params = {
            "TranscriptionJobName": job_name,
            "Media": {"MediaFileUri": media_file_uri},
            "MediaFormat": media_format,
            "LanguageCode": language_code
        }
        if OUTPUT_BUCKET:
            job_params["OutputBucketName"] = OUTPUT_BUCKET

        response = transcribe_client.start_transcription_job(**job_params)
        print("Transcription job submitted successfully:", response)

        return {
            "job_name": job_name,
            "bucket": bucket,
            "key": key
        }

    except Exception as error:
        print("Error submitting transcription job:", error)
        raise error