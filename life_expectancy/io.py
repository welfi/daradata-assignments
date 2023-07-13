import json
import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the data from a TSV or JSON file and return a Pandas DataFrame.
    """
    file_extension = file_path.split(".")[-1].lower()

    if file_extension == "tsv":
        return pd.read_csv(file_path, sep="\t")
    elif file_extension == "json":
        with open(file_path, "r", encoding="utf-8") as json_file:
            json_data = json.load(json_file)
        return pd.DataFrame(json_data)
    else:
        raise ValueError("Invalid file format.")


def save_data(data: pd.DataFrame, file_path: str) -> None:
    """
    Save the cleaned data to a CSV file.
    """
    data.to_csv(file_path, index=False, sep="\t")
