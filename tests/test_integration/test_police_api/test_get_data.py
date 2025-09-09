import json
from pathlib import Path
from typing import Any

import pytest

from src.ingest.police_client import (
    PoliceClient,
)
from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch


@pytest.fixture
def test_data_directory() -> Path:
    return Path("tests/test_integration/test_police_api/test_data")


@pytest.fixture
def expected_forces(test_data_directory: Path) -> list[Force]:
    return [
        Force.model_validate(force)
        for force in get_json_from_file(test_data_directory, "forces.json")
    ]


@pytest.fixture
def expected_available_dates(test_data_directory: Path) -> list[AvailableDate]:
    return [
        AvailableDate.model_validate(force)
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


class TestGetForces:
    @pytest.mark.asyncio
    async def test_get_forces(self, expected_forces: list[Force]):
        police_client = PoliceClient()

        forces = await police_client.get_forces()

        assert forces == expected_forces


class TestGetAvailableDates:
    @pytest.mark.asyncio
    async def test_get_available_dates(
        self, expected_available_dates: list[AvailableDate]
    ):
        police_client = PoliceClient()

        available_dates = await police_client.get_available_dates()

        for expected_date in expected_available_dates:
            assert expected_date in available_dates


class TestGetStopAndSearches:
    @pytest.mark.asyncio
    async def test_get_stop_and_searches_with_location(
        self, expected_stop_and_searches_with_location: list[StopAndSearch]
    ):
        police_client = PoliceClient()
        date = "2023-07"
        force_id = "leicestershire"
        with_location = True

        stop_and_searches = await police_client.get_stop_and_searches(
            date=date, force_id=force_id, with_location=with_location
        )

        assert stop_and_searches == expected_stop_and_searches_with_location

    @pytest.mark.asyncio
    async def test_get_stop_and_searches_without_location(
        self, expected_stop_and_searches_without_location: list[StopAndSearch]
    ):
        police_client = PoliceClient()
        date = "2023-07"
        force_id = "leicestershire"
        with_location = False

        stop_and_searches = await police_client.get_stop_and_searches(
            date=date, force_id=force_id, with_location=with_location
        )

        assert stop_and_searches == expected_stop_and_searches_without_location
