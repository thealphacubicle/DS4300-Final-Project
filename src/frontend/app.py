import streamlit as st
import boto3
import uuid
import io
from mutagen import File


def upload_files_tab():
    st.header("Upload MP3 or WAV File")
    st.markdown("Upload your MP3 or WAV file to start the processing pipeline (AWS integration coming soon).")

    # File uploader widget that accepts only MP3 files
    uploaded_file = st.file_uploader("Choose an MP3 or WAV file", type=["mp3", "wav"])

    if uploaded_file is not None:
        st.info(f"Selected file: **{uploaded_file.name}**")
        unique_file_name = f"{uuid.uuid4()}_{uploaded_file.name}"

        file_bytes = uploaded_file.read()

        try:
            audio = File(io.BytesIO(file_bytes))
            duration = audio.info.length
            bitrate = audio.info.bitrate / 1000  # convert to kbps
            st.markdown(f"**Duration:** {duration:.2f} seconds")
            st.markdown(f"**Bitrate:** {bitrate:.0f} kbps")

        except Exception as e:
            st.warning("Could not read audio metadata.")
            st.error(str(e))

        if st.button("Simulate Upload"):
            # Placeholder action instead of actual AWS S3 upload
            st.success("File 'uploaded' successfully!")
            st.markdown(f"**Simulated S3 Location:** `s3://your-bucket/{unique_file_name}`")


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