import pandas as pd
import shutil
import os

from src.entitlement_framework.file_scanner import get_incoming_files
from src.entitlement_framework.dataset_identifier import extract_dataset_name
from src.entitlement_framework.file_detector import detect_file_format
from src.utils.config_reader import get_pii_columns, get_allowed_formats
from src.entitlement_framework.pii_encryptor import generate_data_key, encrypt_dataframe  # AES-256 + KMS


PROCESSING_FOLDER = "data/processing"
PROCESSED_FOLDER = "data/processed"
FAILED_FOLDER = "data/failed"
ARCHIVE_FOLDER = "data/archive"


def process_files():

    files = get_incoming_files()

    for file_path in files:

        try:

            print(f"Processing {file_path}")
            dataset = extract_dataset_name(file_path)

            file_format = detect_file_format(file_path)

            allowed_formats = get_allowed_formats(dataset)

            if file_format not in allowed_formats:
                raise Exception("Format not allowed")

            # Move file to processing folder
            processing_path = os.path.join(PROCESSING_FOLDER, os.path.basename(file_path))
            shutil.move(file_path, processing_path)

            # Read file
            if file_format == "csv":
                df = pd.read_csv(processing_path)
            elif file_format == "parquet":
                df = pd.read_parquet(processing_path)
            elif file_format == "json":
                df = pd.read_json(processing_path)

            pii_columns = get_pii_columns(dataset)

            # Generate AES-256 data key using AWS KMS
            data_key, encrypted_data_key = generate_data_key()

            # Encrypt PII columns
            df = encrypt_dataframe(df, pii_columns, data_key)

            # Save encrypted data
            output_path = os.path.join(PROCESSED_FOLDER, os.path.basename(processing_path))
            if file_format == "csv":
                df.to_csv(output_path, index=False)
            elif file_format == "parquet":
                df.to_parquet(output_path)
            elif file_format == "json":
                df.to_json(output_path, orient="records")

            # Save encrypted data key alongside the file (optional)
            key_path = output_path + ".key"
            with open(key_path, "wb") as f:
                f.write(encrypted_data_key)

            # Move original file to archive
            shutil.move(processing_path, os.path.join(ARCHIVE_FOLDER, os.path.basename(processing_path)))
            print(f"Success: {output_path}")

        except Exception as e:

            print("Failed:", e)

            shutil.move(file_path, os.path.join(FAILED_FOLDER, os.path.basename(file_path)))