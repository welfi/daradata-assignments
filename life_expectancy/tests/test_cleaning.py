"""Tests for the cleaning module"""
from unittest.mock import patch
import pandas as pd

from life_expectancy.cleaning import (
    unpivot_data,
    clean_data_types,
    filter_data,
    clean_data,
)
from life_expectancy.io import load_data, save_data


def test_clean_data(pt_life_expectancy_expected, eu_life_expectancy_sample):
    """Run the `clean_data` function and compare the output to the expected output"""
    eu_life_expectancy_dataset = eu_life_expectancy_sample
    dataset = clean_data(eu_life_expectancy_dataset, country="PT")
    expected_dataset = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(dataset, expected_dataset)


def test_unpivot_data():
    """test function for unpivot data"""
    sample_data = pd.DataFrame(
        {
            "unit,sex,age,geo\\time": ["U1,M,O,A1,PT", "U2,F,Y,A2,DE"],
            "value_1": [10, 20],
            "value_2": [30, 40],
        }
    )

    expected_output = pd.DataFrame(
        {
            "unit": ["U1", "U2", "U1", "U2"],
            "sex": ["M", "F", "M", "F"],
            "age": ["O", "Y", "O", "Y"],
            "region": ["A1", "A2", "A1", "A2"],
            "year": ["value_1", "value_1", "value_2", "value_2"],
            "value": [10, 20, 30, 40],
        }
    )

    result = unpivot_data(sample_data)

    pd.testing.assert_frame_equal(result, expected_output)


def test_clean_data_types():
    """test function for clean data types"""
    data = pd.DataFrame({"year": [2020, 2021], "value": ["10.5%", "20.3%"]})
    expected = pd.DataFrame({"year": [2020, 2021], "value": [10.5, 20.3]})
    result = clean_data_types(data)
    pd.testing.assert_frame_equal(result, expected)


def test_filter_data():
    """test function for filter data"""
    data = pd.DataFrame(
        {
            "region": ["region1", "region2", "region1", "region3"],
            "value": [10, 20, 30, 40],
        }
    )
    expected = pd.DataFrame({"region": ["region1", "region1"], "value": [10, 30]})
    result = filter_data(data, "region1")
    result = result.reset_index(drop=True)
    pd.testing.assert_frame_equal(result, expected)


@patch("life_expectancy.io.pd.read_csv")
def test_load_data(mock_read_csv):
    """test function for load data"""
    mock_read_csv.return_value = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    result = load_data("test.csv")
    mock_read_csv.assert_called_once_with("test.csv", sep="\t")
    expected = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    pd.testing.assert_frame_equal(result, expected)


@patch("life_expectancy.io.pd.DataFrame.to_csv")
def test_save_data(mock_to_csv):
    """test function for save data"""
    data = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    save_data(data, "test.csv")
    mock_to_csv.assert_called_once_with("test.csv", index=False)
