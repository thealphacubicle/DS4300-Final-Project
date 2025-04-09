import os
import boto3
from dotenv import load_dotenv

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

def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Uploads a file to an S3 bucket.

    :param file_name: The file to upload.
    :param bucket: The bucket to upload to.
    :param object_name: S3 object name. If not specified then file_name is used.
    :return: True if file was uploaded, else False.
    """
    if object_name is None:
        object_name = file_name

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Successfully uploaded '{file_name}' to bucket '{bucket}' as '{object_name}'")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

if __name__ == '__main__':
    # Define the folder containing the files to upload
    folder_path = input("Enter the folder path containing files to upload: ")

    # Ensure the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
    else:
        # Iterate through all files in the folder
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):  # Ensure it's a file
                # Upload the file to the S3 landing bucket specified in .env
                upload_file_to_s3(file_path, S3_LANDING_BUCKET, file_name)
