import yaml

CONFIG_PATH = "config/pii_config.yaml"

def load_config():

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_dataset_config(dataset):

    config = load_config()

    datasets = config.get("datasets", {})

    if dataset not in datasets:
        raise Exception(f"Dataset {dataset} not found in config")

    return datasets[dataset]


def get_pii_columns(dataset):

    dataset_config = get_dataset_config(dataset)

    return dataset_config.get("pii_columns", [])


def get_allowed_formats(dataset):

    dataset_config = get_dataset_config(dataset)

    return dataset_config.get("allowed_formats", [])