import streamlit as st
import boto3
import uuid
import io
from mutagen import File
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

def upload_files_tab():
    st.header("Upload MP3 or WAV File")
    st.markdown("Upload your MP3 or WAV file to start the processing pipeline (now with AWS S3 integration).")

    uploaded_file = st.file_uploader("Choose an MP3 or WAV file", type=["mp3", "wav"])

    if uploaded_file is not None:
        st.info(f"Selected file: **{uploaded_file.name}**")
        unique_file_name = f"{uuid.uuid4()}_{uploaded_file.name}"
        file_bytes = uploaded_file.read()

        try:
            audio = File(io.BytesIO(file_bytes))
            duration = audio.info.length
            bitrate = audio.info.bitrate / 1000
            st.markdown(f"**Duration:** {duration:.2f} seconds")
            st.markdown(f"**Bitrate:** {bitrate:.0f} kbps")
        except Exception as e:
            st.warning("Could not read audio metadata.")
            st.error(str(e))

        BUCKET_NAME = "sample-upload-bucket-neu01"

        if st.button("Upload to S3"):
            try:
                s3 = boto3.client(
                    "s3",
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                    region_name=os.getenv("AWS_DEFAULT_REGION")
                )

                s3.upload_fileobj(io.BytesIO(file_bytes), BUCKET_NAME, unique_file_name)

                st.success("File uploaded to S3 successfully!")
                st.markdown(f"**S3 Path:** `s3://{BUCKET_NAME}/{unique_file_name}`")

            except Exception as e:
                st.error("Upload to S3 failed.")
                st.error(str(e))


def dashboard_tab():
    st.header("Analytics Dashboard")
    st.markdown(
        "This is a placeholder for the analytics dashboard. Once integrated, you can display charts, metrics, and other insights here.")
    st.info("Dashboard functionality coming soon...")


def main():
    st.set_page_config(page_title="Audio Processing Pipeline", layout="wide")
    st.title("Audio Processing Pipeline")

    # Create two tabs: one for Upload and one for Dashboard
    tab1, tab2 = st.tabs(["Upload Files", "Dashboard"])

    with tab1:
        upload_files_tab()

    with tab2:
        dashboard_tab()


if __name__ == "__main__":
    main()