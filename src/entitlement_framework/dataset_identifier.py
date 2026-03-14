import os

def extract_dataset_name(file_path):

    file_name = os.path.basename(file_path)

    dataset = file_name.split("_")[0]

    return dataset