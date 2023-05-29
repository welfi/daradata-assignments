import argparse
import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the data from a TSV file and return a Pandas DataFrame.
    """
    return pd.read_csv(file_path, sep='\t')


def unpivot_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot the data by splitting the 'unit,sex,age,geo\\time' column
    into separate columns and melt the DataFrame.
    """
    split_columns = data['unit,sex,age,geo\\time'].str.split(',', expand=True)
    data['unit'] = split_columns[0]
    data['sex'] = split_columns[1]
    data['age'] = split_columns[2]
    data['geo\\time'] = split_columns[3]
    data = data.drop(columns=['unit,sex,age,geo\\time'])
    data = pd.melt(data, id_vars=['unit', 'sex', 'age', 'geo\\time'], var_name='year', value_name='value')
    return data


def clean_data_types(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data types of the 'year' and 'value' columns.
    Convert 'year' to int and 'value' to float.
    Remove rows with NaN values in 'value' column.
    """
    data['year'] = data['year'].astype(int)
    data['value'] = pd.to_numeric(data['value'], errors='coerce')
    data = data.dropna(subset=['value'])
    return data


def filter_data(data: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Filter the data to include rows where 'geo\time' column is equal to the given region.
    """
    return data[data['geo\\time'] == region]


def save_cleaned_data(data: pd.DataFrame, file_path: str) -> None:
    """
    Save the cleaned data to a CSV file.
    """
    data.to_csv(file_path, index=False)


def clean_data(country: str = 'PT') -> None:
    """
    Clean the data by performing loading, unpivot, cleaning data types,
    filtering, and saving the cleaned data.
    """
    data = load_data('life_expectancy/data/eu_life_expectancy_raw.tsv')
    data = unpivot_data(data)
    data = clean_data_types(data)
    data = filter_data(data, country)
    save_cleaned_data(data, 'life_expectancy/data/pt_life_expectancy.csv')


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description='Clean life expectancy data')
    parser.add_argument('--country', default='PT', help='Country code to filter the data (default: PT)')
    args = parser.parse_args()
    clean_data(args.country)
