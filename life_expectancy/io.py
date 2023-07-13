import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the data from a TSV file and return a Pandas DataFrame.
    """
    return pd.read_csv(file_path, sep="\t")


def save_data(data: pd.DataFrame, file_path: str) -> None:
    """
    Save the cleaned data to a CSV file.
    """
    data.to_csv(file_path, index=False)
