import os

def detect_file_format(file_path):

    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return "csv"

    elif ext == ".parquet":
        return "parquet"

    elif ext == ".json":
        return "json"

    else:
        raise ValueError(f"Unsupported format {ext}")