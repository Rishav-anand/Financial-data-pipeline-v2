import boto3
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os
import base64

# Initialize KMS client
kms_client = boto3.client("kms", region_name="us-east-1")  # e.g., 'ap-south-1'

# Your AWS KMS CMK Key ID or ARN
KMS_KEY_ID = "arn:aws:kms:us-east-1:038462784041:key/fbe15667-7665-4813-b4b5-abcfa67c75fd"


def generate_data_key():
    """
    Generate a 256-bit AES key using AWS KMS
    Returns:
        plaintext_key: bytes
        encrypted_key: bytes
    """
    response = kms_client.generate_data_key(
        KeyId=KMS_KEY_ID,
        KeySpec="AES_256"
    )
    plaintext_key = response["Plaintext"]
    encrypted_key = response["CiphertextBlob"]
    return plaintext_key, encrypted_key


def encrypt_value(value, data_key):
    """
    Encrypt a single value using AES-256-CBC
    """
    if value is None:
        return None

    iv = os.urandom(16)  # Initialization vector
    cipher = Cipher(algorithms.AES(data_key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # PKCS7 padding
    data = str(value).encode("utf-8")
    pad_len = 16 - (len(data) % 16)
    data += bytes([pad_len] * pad_len)

    ct = encryptor.update(data) + encryptor.finalize()

    # Return IV + ciphertext encoded in base64
    return base64.b64encode(iv + ct).decode("utf-8")


def encrypt_dataframe(df, pii_columns, data_key):
    """
    Encrypt all PII columns in the dataframe
    """
    for col in pii_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: encrypt_value(x, data_key))
    return df