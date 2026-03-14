import os

RAW_FOLDER = "data/raw"

def get_incoming_files():

    files = []

    if not os.path.exists(RAW_FOLDER):
        return files

    for file in os.listdir(RAW_FOLDER):
        path = os.path.join(RAW_FOLDER, file)
        if os.path.isfile(path):
            files.append(path)

    return files