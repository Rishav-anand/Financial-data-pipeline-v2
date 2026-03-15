import pandas as pd
import boto3
import os
import tempfile

from src.entitlement_framework.file_scanner import get_incoming_files
from src.entitlement_framework.dataset_identifier import extract_dataset_name
from src.entitlement_framework.file_detector import detect_file_format
from src.utils.config_reader import get_pii_columns, get_allowed_formats
from src.entitlement_framework.pii_encryptor import generate_data_key, encrypt_dataframe


# S3 CONFIG
S3_BUCKET = "financial-data-pipeline-project-v2"

RAW_PREFIX = "raw/"
PROCESSING_PREFIX = "processing/"
PROCESSED_PREFIX = "processed/"
ARCHIVE_PREFIX = "archive/"
FAILED_PREFIX = "failed/"


s3 = boto3.client("s3")


def download_file_from_s3(s3_key):

    local_path = os.path.join(tempfile.gettempdir(), os.path.basename(s3_key))

    s3.download_file(S3_BUCKET, s3_key, local_path)

    return local_path


def upload_file_to_s3(local_path, s3_key):

    s3.upload_file(local_path, S3_BUCKET, s3_key)


def move_s3_file(source_key, target_key):

    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={'Bucket': S3_BUCKET, 'Key': source_key},
        Key=target_key
    )

    s3.delete_object(Bucket=S3_BUCKET, Key=source_key)


def process_files():

    files = get_incoming_files()

    for file_key in files:

        try:

            print(f"Processing {file_key}")

            dataset = extract_dataset_name(file_key)

            file_format = detect_file_format(file_key)

            allowed_formats = get_allowed_formats(dataset)

            if file_format not in allowed_formats:
                raise Exception("Format not allowed")

            # Download file locally
            local_file = download_file_from_s3(file_key)

            # Read file
            if file_format == "csv":
                df = pd.read_csv(local_file)

            elif file_format == "parquet":
                df = pd.read_parquet(local_file)

            elif file_format == "json":
                df = pd.read_json(local_file)

            pii_columns = get_pii_columns(dataset)

            # Generate AES256 data key using KMS
            data_key, encrypted_data_key = generate_data_key()

            # Encrypt PII columns
            df = encrypt_dataframe(df, pii_columns, data_key)

            # Save encrypted data locally
            output_file = os.path.join(tempfile.gettempdir(), os.path.basename(local_file))

            if file_format == "csv":
                df.to_csv(output_file, index=False)

            elif file_format == "parquet":
                df.to_parquet(output_file)

            elif file_format == "json":
                df.to_json(output_file, orient="records")

            # Upload encrypted file to S3 processed folder
            processed_key = PROCESSED_PREFIX + os.path.basename(file_key)

            upload_file_to_s3(output_file, processed_key)

            # Save encrypted data key file
            key_file = output_file + ".key"

            with open(key_file, "wb") as f:
                f.write(encrypted_data_key)

            upload_file_to_s3(key_file, processed_key + ".key")

            # Move original file to archive
            archive_key = ARCHIVE_PREFIX + os.path.basename(file_key)

            move_s3_file(file_key, archive_key)

            print(f"Success → {processed_key}")

        except Exception as e:

            print("Failed:", e)

            try:

                failed_key = FAILED_PREFIX + os.path.basename(file_key)

                move_s3_file(file_key, failed_key)

            except Exception as move_error:

                print("Failed to move file to failed folder:", move_error)