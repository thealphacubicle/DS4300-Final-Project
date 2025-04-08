import os
import boto3
import random
import string
from dotenv import load_dotenv
import uuid


# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
S3_LANDING_BUCKET = os.getenv('S3_LANDING_BUCKET')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')

# Create an S3 client using the credentials and region from the env file
s3_client = boto3.client(
    's3',
    region_name=AWS_DEFAULT_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)


def create_random_file(filename, size=100):
    """
    Create a file with random letters and digits, appending a UUID for uniqueness.
    :param filename: Base name of the file to create.
    :param size: Number of random characters to write to the file.
    """
    unique_filename = f"{filename}_{uuid.uuid4()}"
    random_text = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
    with open(unique_filename, 'w') as file:
        file.write(random_text)
    print(f"Random file '{unique_filename}' created with {size} characters.")
    return unique_filename


def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Uploads a file to an S3 bucket.

    :param file_name: The file to upload.
    :param bucket: The bucket to upload to.
    :param object_name: S3 object name. If not specified then file_name is used.
    """
    if object_name is None:
        object_name = file_name

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Successfully uploaded '{file_name}' to bucket '{bucket}' as '{object_name}'")
    except Exception as e:
        print(f"Error uploading file: {e}")


if __name__ == '__main__':
    # Define the base file name
    base_file_name = "random_file.txt"

    # Create a random file locally with a unique name
    unique_file_name = create_random_file(base_file_name, size=100)

    # Upload the file to the S3 landing bucket specified in .env
    upload_file_to_s3(unique_file_name, S3_LANDING_BUCKET)
