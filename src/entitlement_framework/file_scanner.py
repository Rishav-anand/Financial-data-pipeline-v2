import boto3

# S3 configuration
S3_BUCKET = "financial-data-pipeline-project-v2"
RAW_PREFIX = "raw/"

s3 = boto3.client("s3")

def get_incoming_files():
    files = []
    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX
    )
    if "Contents" not in response:
        return files
    for obj in response["Contents"]:
        key = obj["Key"]
        # skip folder itself
        if key.endswith("/"):
            continue
        files.append(key)

    return files