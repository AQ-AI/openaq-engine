import datetime
from dateutil.relativedelta import relativedelta

from src.time_splitter import TimeSplitter


def test_get_end_time_windows(mocker):
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    window_start_date = datetime.date(2020, 1, 1)

    # Call the method and get the end date
    end_date = time_splitter._get_end_time_windows(window_start_date)

    # Assert the expected end date is returned
    assert end_date == datetime.date(2020, 3, 1)


def test_get_validation_window(mocker):
    """Tests the get_validation_window method"""
    mocker.patch("src.time_splitter.mlflow")
    # Mock the _get_start_time_windows and _get_end_time_windows methods
    mocker.patch.object(
        TimeSplitter,
        "_get_start_time_windows",
        return_value=datetime.date(2020, 1, 1),
    )
    mocker.patch.object(
        TimeSplitter,
        "_get_end_time_windows",
        return_value=datetime.date(2020, 3, 1),
    )

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    end_date = datetime.date(2020, 6, 1)
    window_no = 2

    # Call the method and assert the expected start and end dates are returned
    start_date, end_date = time_splitter.get_validation_window(
        end_date, window_no
    )
    assert start_date == datetime.date(2020, 1, 1)
    assert end_date == datetime.date(2020, 3, 1)


def test_get_start_time_windows():
    """Tests the _get_start_time_windows method"""
    # Mock the relativedelta method to return expected start dates

    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )
    end_date = datetime.date(2020, 12, 1)

    # Call the method twice and assert the expected start dates are returned
    start_date = time_splitter._get_start_time_windows(end_date, 0)
    assert isinstance(start_date, datetime.date)

    start_date = time_splitter._get_start_time_windows(end_date, 1)
    assert isinstance(start_date, datetime.date)


def test_execute_for_openaq_aws(mocker):
    params = {"mocked": "params"}
    city = "London"
    country = "UK"
    pollutant = "pm25"
    latest_date = "2020-01-01"
    # Mock the calls to create_end_date_from_aws and create_start_date_from_aws
    mocker.patch.object(
        TimeSplitter,
        "create_end_date_from_aws",
        return_value=datetime.date(2020, 1, 1),
    )
    mocker.patch.object(
        TimeSplitter,
        "create_start_date_from_aws",
        return_value=datetime.date(2019, 6, 1),
    )

    # Instantiate TimeSplitter
    time_splitter = TimeSplitter(
        time_window_length=6,
        within_window_sampler=2,
        window_count=3,
        train_validation_dict={},
        target_variable="pm25",
        country="UK",
        source="openaq-api",
    )

    # Call execute_for_openaq_aws and get results
    end_date, start_date = time_splitter.execute_for_openaq_aws(
        params, city, country, pollutant, latest_date
    )

    # Assert expected end_date and start_date are returned from the mocked methods
    assert end_date == datetime.date(2020, 1, 1)
    assert start_date == datetime.date(2019, 6, 1)
