import streamlit as st
import boto3
import uuid
import io
from mutagen import File
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px


load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

def upload_files_tab():
    uploaded_files = st.file_uploader("Choose MP3 or WAV files", type=["mp3", "wav"], accept_multiple_files=True)

    if uploaded_files:
        file_metadata = []

        for uploaded_file in uploaded_files:
            st.info(f"Selected file: **{uploaded_file.name}**")
            unique_file_name = f"{uuid.uuid4()}_{uploaded_file.name}"
            file_bytes = uploaded_file.read()

            try:
                audio = File(io.BytesIO(file_bytes))
                duration = audio.info.length
                bitrate = audio.info.bitrate / 1000
                st.markdown(f"**Duration:** {duration:.2f} seconds")
                st.markdown(f"**Bitrate:** {bitrate:.0f} kbps")
                file_metadata.append((unique_file_name, file_bytes))
            except Exception as e:
                st.warning(f"Could not read audio metadata for file: {uploaded_file.name}")
                st.error(str(e))
                continue

        BUCKET_NAME = os.environ["S3_LANDING_BUCKET"]

        if st.button("Upload all files to S3"):
            try:
                s3 = boto3.client(
                    "s3",
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                    region_name=os.getenv("AWS_DEFAULT_REGION")
                )

                for unique_file_name, file_bytes in file_metadata:
                    s3.upload_fileobj(io.BytesIO(file_bytes), BUCKET_NAME, unique_file_name)

                st.success("All files uploaded to S3 successfully!")
            except Exception as e:
                st.error("Upload to S3 failed for one or more files.")
                st.error(str(e))

def dashboard_tab():
    st.title("Analytics Dashboard")
    st.markdown(
        """
        Welcome to your Analytics Dashboard! Explore insights from your transcription data through interactive charts.
        The visualizations below highlight trends in sentiment over time, the distribution of sentiment scores, the breakdown
        by file type, and how transcription length might relate to sentiment.
        """
    )

    # Retrieve AWS RDS MySQL credentials from environment variables
    RDS_USER = os.environ["RDS_USER"]
    RDS_PASSWORD = os.environ["RDS_PASSWORD"]
    RDS_HOST = os.environ["RDS_HOST"]
    RDS_PORT = int(os.environ["RDS_PORT"])
    RDS_DB = os.environ["RDS_DB"]

    # Create a SQLAlchemy engine using the PyMySQL driver
    connection_uri = f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}"
    engine = create_engine(connection_uri)

    # SQL query using fully qualified table name and backticks for identifiers
    query = """
        SELECT 
            `id`,
            `audio_file_name`,
            `file_type`,
            `transcription_text`,
            `transcription_sentiment_score`,
            `created_at`
        FROM `speech_processing_db`.`transcriptions`
        ORDER BY `created_at`
    """

    try:
        # Retrieve the data into a DataFrame
        df = pd.read_sql(query, engine)
        st.success("Data pulled successfully from RDS!")

        if not df.empty:
            # Preprocessing steps
            df["created_at"] = pd.to_datetime(df["created_at"])

            # Create a new column for transcription length
            df["transcription_length"] = df["transcription_text"].apply(lambda x: len(x) if isinstance(x, str) else 0)

            # Display key metrics in a row
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
                st.metric("Total Records", len(df))
                st.markdown("</div>", unsafe_allow_html=True)

            with col2:
                st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
                st.metric("Unique File Types", df["file_type"].nunique())
                st.markdown("</div>", unsafe_allow_html=True)


            # Existing visualizations
            # KDE plot for sentiment scores over time
            fig_kde_sentiment = px.density_contour(
                df,
                x="created_at",
                y="transcription_sentiment_score",
                title="KDE Plot of Sentiment Scores Over Time",
                labels={"created_at": "Created At", "transcription_sentiment_score": "Sentiment Score"}
            )
            fig_kde_sentiment.update_traces(contours_coloring="fill", showlegend=True)
            st.plotly_chart(fig_kde_sentiment, use_container_width=True)

            # --- New Box Plots Section ---
            st.markdown("## Box Plots")

            # Box plot for the distribution of sentiment scores
            fig_sentiment = px.box(
                df,
                x="transcription_sentiment_score",
                title="Distribution of Sentiment Scores",
                labels={"transcription_sentiment_score": "Sentiment Score"}
            )
            st.plotly_chart(fig_sentiment, use_container_width=True)

            # Box plot for the distribution of transcription lengths
            fig_length = px.box(
                df,
                x="transcription_length",
                title="Distribution of Transcription Lengths"
            )
            st.plotly_chart(fig_length, use_container_width=True)

        else:
            st.info("No data to display.")
    except Exception as e:
        st.error("Failed to retrieve data from RDS.")
        st.error(str(e))

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