import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
from src.utils.utils import (
    api_response_to_df,
    ee_array_to_df,
    extract_utc_date,
    get_categorical_feature_indices,
    get_s3_file_path_list,
    json_provider,
    query_results_from_aws,
    read_csv,
    write_csv,
    write_dataclass,
    write_to_db,
)


def test_read_csv(mocker):
    mocker.patch(
        "pandas.read_csv", return_value=pd.DataFrame({"col": [1, 2, 3]})
    )
    df = read_csv("dummy_path")
    assert df.equals(pd.DataFrame({"col": [1, 2, 3]}))


def test_write_csv(mocker):
    mocker.patch("pandas.DataFrame.to_csv")
    df = pd.DataFrame({"col": [1, 2, 3]})
    write_csv(df, "dummy_path")
    df.to_csv.assert_called_once_with(
        "dummy_path",
        index=False,
        na_rep="",
        sep=",",
        lineterminator="\n",
        encoding="utf-8",
        escapechar="\r",
    )


def test_api_response_to_df(mocker):
    response_data = {
        "results": [{"id": 1, "value": 42}, {"id": 2, "value": 99}]
    }
    mocker.patch(
        "requests.get",
        return_value=MagicMock(
            status_code=200, text=json.dumps(response_data)
        ),
    )

    url = "http://fakeurl.com"
    df = api_response_to_df(url)
    expected_df = pd.DataFrame(response_data["results"])
    assert df.equals(expected_df)


def test_query_results_from_aws(mocker):
    params = {
        "region": "us-east-1",
        "bucket": "fakebucket",
        "path": "fake_path",
    }
    query = "SELECT * FROM fake_table"

    # Mock the Athena client and its methods
    mock_athena_client = MagicMock()
    mock_athena_client.start_query_execution.return_value = {
        "QueryExecutionId": "12345"
    }
    mock_athena_client.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "RUNNING"}}},
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
    ]
    mock_athena_client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "row1"}]},
                {"Data": [{"VarCharValue": "row2"}]},
            ]
        }
    }

    # Patch boto3 client creation to return the mock client
    with patch("boto3.Session.client", return_value=mock_athena_client):
        result = query_results_from_aws(params, query)
        print("response_query_result", result)
        data = [row["Data"][0]["VarCharValue"] for row in result["Rows"]]
        assert "row1" in data
        assert "row2" in data


def test_get_s3_file_path_list(mocker):
    mock_bucket = MagicMock()
    mock_resource = MagicMock(Bucket=MagicMock(return_value=mock_bucket))
    mock_objects = [MagicMock(key="file1.csv"), MagicMock(key="file2.csv")]
    mock_bucket.objects.filter.return_value = mock_objects

    bucket = "fake_bucket"
    folder = "fake_folder"
    file_list = get_s3_file_path_list(mock_resource, bucket, folder)
    assert file_list == ["file1.csv", "file2.csv"]


def test_write_dataclass(mocker):
    mock_open = mocker.patch("builtins.open", mocker.mock_open())
    mock_json = mocker.patch("json.dumps", return_value="{}")

    class DataClassExample:
        def __init__(self, name):
            self.name = name

    obj = DataClassExample(name="test")
    write_dataclass(obj, "dummy_path")
    mock_open.assert_called_once_with("dummy_path", "w+")
    mock_json.assert_called_once_with(
        obj, indent=4, ensure_ascii=True, default=mocker.ANY
    )


def test_get_categorical_feature_indices():
    df = pd.DataFrame({"A": pd.Categorical(["a", "b", "a"]), "B": [1, 2, 3]})
    indices = get_categorical_feature_indices(df)
    assert indices == [0]


def test_json_provider(mocker):
    mock_open = mocker.patch(
        "builtins.open", mocker.mock_open(read_data='{"key": "value"}')
    )
    result = json_provider("dummy_path", "dummy_cmd")
    assert result == {"key": "value"}
    mock_open.assert_called_once_with("dummy_path")


def test_extract_utc_date():
    date_dict = json.dumps(
        {
            "utc": "2023-03-31T23:30:00+00:00",
            "local": "2023-03-31T23:30:00+05:30",
        }
    )
    date = extract_utc_date(date_dict)
    assert date == datetime(2023, 3, 31).date()


def test_write_to_db(mocker):
    mocker.patch("pandas.DataFrame.to_sql")
    df = pd.DataFrame({"col": [1, 2, 3]})
    engine = MagicMock()
    write_to_db(df, engine, "table", "schema", "replace")
    df.to_sql.assert_called_once_with(
        name="table",
        schema="schema",
        con=engine,
        if_exists="replace",
        index=False,
    )


def test_ee_array_to_df():
    arr = [
        ["id", "longitude", "latitude", "time", "band1", "band2"],
        [
            "2015040100F003",
            106.87548887637286,
            47.87521899374363,
            1427846400000,
            0.1,
            0.2,
        ],
        [
            "2015040100F006",
            106.87548887637286,
            47.87521899374363,
            1427846400000,
            0.3,
            0.4,
        ],
    ]
    list_of_bands = ["band1", "band2"]
    df = ee_array_to_df(arr, list_of_bands)

    expected_df = pd.DataFrame(
        {
            "longitude": [106.87548887637286, 106.87548887637286],
            "latitude": [47.87521899374363, 47.87521899374363],
            "time": [1427846400000, 1427846400000],
            "datetime": [pd.Timestamp(2015, 4, 1), pd.Timestamp(2015, 4, 1)],
            "band1": [0.1, 0.3],
            "band2": [0.2, 0.4],
        }
    )

    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_df)
    pd.testing.assert_frame_equal(df, expected_df)
