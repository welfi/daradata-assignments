import argparse
import re
from abc import ABC, abstractmethod
import pandas as pd
from life_expectancy.io import load_data, save_data


class Strategy(ABC):
    """
    Strategy abstract base class
    """

    @abstractmethod
    def process(self, data):
        """
        Process function for strategy classes
        """


class JsonDataProcessor(Strategy):
    """
    This class is the JSON format processor
    """

    @staticmethod
    def filter_data(data: pd.DataFrame, region: str) -> pd.DataFrame:
        """
        Filter the data to include rows where 'geo\time' column is equal to the given region.
        """
        return data[data["region"] == region]

    def process(self, data: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
        data = self.filter_data(data, country)
        data = data.reset_index(drop=True)
        return data


class CsvDataProcessor:
    """
    This class is the CSV format processor
    """

    @staticmethod
    def unpivot_data(data: pd.DataFrame) -> pd.DataFrame:
        """
        Unpivot the data by splitting the 'unit,sex,age,geo\\time' column
        into separate columns and melt the DataFrame.
        """
        split_columns = data["unit,sex,age,geo\\time"].str.split(",", expand=True)
        data["unit"] = split_columns[0]
        data["sex"] = split_columns[1]
        data["age"] = split_columns[2]
        data["region"] = split_columns[3]
        data = data.drop(columns=["unit,sex,age,geo\\time"])
        data = pd.melt(
            data,
            id_vars=["unit", "sex", "age", "region"],
            var_name="year",
            value_name="value",
        )
        return data

    @staticmethod
    def clean_data_types(data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the data types of the 'year' and 'value' columns.
        Convert 'year' to int and 'value' to float.
        Remove rows with NaN values in 'value' column.
        """
        data["year"] = data["year"].astype("int64")
        data["value"] = data["value"].apply(lambda x: re.sub("[^\\d.]", "", x))
        data["value"] = pd.to_numeric(data["value"], errors="coerce")
        data = data.dropna(subset=["value"])
        return data

    @staticmethod
    def filter_data(data: pd.DataFrame, region: str) -> pd.DataFrame:
        """
        Filter the data to include rows where 'geo\time' column is equal to the given region.
        """
        return data[data["region"] == region]

    def process(self, data: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
        """
        Clean the data by performing loading, unpivot, cleaning data types
        and filtering
        """
        data = self.unpivot_data(data)
        data = self.clean_data_types(data)
        data = self.filter_data(data, country)
        data = data.reset_index(drop=True)
        return data


class StrategyFactory:
    """
    Strategy factory class to select the suitable strategy
    """

    @staticmethod
    def create_strategy(data_format):
        """
        Function that returns the suitable strategy
        """
        if data_format == "csv":
            return CsvDataProcessor()
        if data_format == "json":
            return JsonDataProcessor()
        raise ValueError("Invalid data format")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean life expectancy data")
    parser.add_argument(
        "--country", default="PT", help="Country code to filter the data (default: PT)"
    )
    parser.add_argument(
        "--load_path",
        default="life_expectancy/data/eu_life_expectancy_raw.tsv",
        help="Data file path",
    )
    parser.add_argument(
        "--save_path", default="life_expectancy/data/pt_life_expectancy.csv"
    )
    parser.add_argument(
        "--data_format",
        default="csv",
        choices=["csv", "json"],
        help="Data format (csv or json)",
    )
    args = parser.parse_args()

    strategy = StrategyFactory.create_strategy(args.data_format)
    dataset = load_data(args.load_path)
    processed_data = strategy.process(dataset)
    save_data(processed_data, args.save_path)
