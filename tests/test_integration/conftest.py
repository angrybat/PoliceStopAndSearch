import json
from pathlib import Path
from typing import Any

import pytest

from src.models.bronze.available_date import AvailableDateWithForceIds
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch


@pytest.fixture
def test_data_directory() -> Path:
    return Path("tests/test_integration/test_data")


@pytest.fixture
def expected_forces(test_data_directory: Path) -> list[Force]:
    return [
        Force.model_validate(force)
        for force in get_json_from_file(test_data_directory, "forces.json")
    ]


@pytest.fixture
def expected_available_dates(
    test_data_directory: Path,
) -> list[AvailableDateWithForceIds]:
    return [
        AvailableDateWithForceIds.model_validate(force)
        for force in get_json_from_file(test_data_directory, "available_dates.json")
    ]


@pytest.fixture
def expected_stop_and_searches_with_location(
    test_data_directory: Path,
) -> list[StopAndSearch]:
    return [
        StopAndSearch.model_validate(stop_and_search)
        for stop_and_search in get_json_from_file(
            test_data_directory, "stop_and_searches_with_location.json"
        )
    ]


@pytest.fixture
def expected_stop_and_searches_without_location(
    test_data_directory: Path,
) -> list[StopAndSearch]:
    return [
        StopAndSearch.model_validate(stop_and_search)
        for stop_and_search in get_json_from_file(
            test_data_directory, "stop_and_searches_without_location.json"
        )
    ]


def get_json_from_file(
    test_data_directory: Path, file_name: str
) -> list[dict[str, Any]]:
    expected_data_path = test_data_directory / file_name
    with expected_data_path.open("r") as file:
        return json.load(file)
