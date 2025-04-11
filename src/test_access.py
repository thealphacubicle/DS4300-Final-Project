import json
import boto3
import os
import uuid

# Initialize the AWS service clients
transcribe_client = boto3.client('transcribe')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    """
    Lambda function triggered by an S3 event that starts an AWS Transcribe job,
    then publishes a notification to an SNS topic with metadata about the job.
    """
    try:
        # Extract S3 bucket and object key (assumes one record per event)
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # Construct the S3 URI for the media file
        media_uri = f"s3://{bucket}/{object_key}"

        # Generate a unique transcription job name
        job_name = f"TranscriptionJob-{uuid.uuid4()}"
        file_type = object_key.split(".")[-1].lower()

        # Determine media format and start the transcription job; supports only MP3 and WAV files.
        if file_type == "mp3":
            response = transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': media_uri},
                MediaFormat="mp3",
                LanguageCode=os.getenv("TRANSCRIBE_LANGUAGE_CODE", "en-US")
            )
        elif file_type == "wav":
            response = transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': media_uri},
                MediaFormat="wav",
                LanguageCode=os.getenv("TRANSCRIBE_LANGUAGE_CODE", "en-US")
            )
        else:
            # Log and raise an error if the file type is not supported.
            error_message = f"Unsupported file type: {file_type}. Pipeline only supports MP3 and WAV files!"
            print(error_message)
            raise ValueError(error_message)

        # Construct the payload with transcription job metadata for downstream processing
        result = {
            "audio_file_name": object_key,
            "audio_file_type": file_type,
            "source_link": media_uri,
            "transcription_job_name": response['TranscriptionJob']['TranscriptionJobName']
        }

        # Publish the payload to the SNS topic if SNS_TOPIC_ARN is set
        sns_topic_arn = os.getenv("SNS_TOPIC_ARN")
        if sns_topic_arn:
            sns_response = sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(result),
                Subject="Transcription Job Started"
            )
            print(f"Published to SNS. Response: {sns_response}")
        else:
            print("SNS_TOPIC_ARN is not set in environment variables. Skipping SNS publish.")

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        # Log the error and re-raise for Lambda error reporting/monitoring
        print("Error starting transcription job:", str(e))
        raise e