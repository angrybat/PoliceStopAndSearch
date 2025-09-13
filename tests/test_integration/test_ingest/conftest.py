import json
from pathlib import Path
from typing import Any

import pytest

from src.police_api_ingester.models import (
    AvailableDateWithForceIds,
    Force,
    StopAndSearch,
)


@pytest.fixture
def test_data_directory() -> Path:
    return Path("tests/test_integration/test_ingest/test_data")


@pytest.fixture
def expected_forces(test_data_directory: Path) -> list[Force]:
    return [
        Force.model_validate(force)
        for force in get_json_from_file(test_data_directory, "forces.json")
    ]


@pytest.fixture
def expected_forces_without_btp(expected_forces: list[Force]):
    return [force for force in expected_forces if force.id != "btp"]


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
    stop_and_searches = [
        StopAndSearch.model_validate(stop_and_search)
        for stop_and_search in get_json_from_file(
            test_data_directory, "stop_and_searches_with_location.json"
        )
    ]
    return set_force(stop_and_searches, "leicestershire")


@pytest.fixture
def expected_stop_and_searches_without_location(
    test_data_directory: Path,
) -> list[StopAndSearch]:
    stop_and_searches = [
        StopAndSearch.model_validate(stop_and_search)
        for stop_and_search in get_json_from_file(
            test_data_directory, "stop_and_searches_without_location.json"
        )
    ]
    return set_force(stop_and_searches, "leicestershire")


@pytest.fixture
def expected_stop_and_searches(test_data_directory: Path) -> list[StopAndSearch]:
    return [
        StopAndSearch.model_validate(stop_and_search)
        for stop_and_search in get_json_from_file(
            test_data_directory, "stop_and_searches_2024-01.json"
        )
    ]


def get_json_from_file(
    test_data_directory: Path, file_name: str
) -> list[dict[str, Any]]:
    expected_data_path = test_data_directory / file_name
    with expected_data_path.open("r") as file:
        return json.load(file)


def set_force(
    stop_and_searches: list[StopAndSearch], force_id: str
) -> list[StopAndSearch]:
    for stop_and_search in stop_and_searches:
        stop_and_search.force_id = force_id
    return stop_and_searches
