import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import pytest
from sqlmodel import SQLModel

from src.ingest.police_client import (
    PoliceClient,
    get_available_date,
    get_force,
    get_stop_and_search,
)
from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch


@pytest.fixture
def test_data_directory() -> Path:
    return Path("tests/test_integration/test_police_api/test_data")


@pytest.fixture
def expected_forces(test_data_directory: Path) -> list[Force]:
    return get_data_from_file(test_data_directory, "forces.json", get_force)


@pytest.fixture
def expected_available_dates(test_data_directory: Path) -> list[AvailableDate]:
    return get_data_from_file(
        test_data_directory, "available_dates.json", get_available_date
    )


@pytest.fixture
def expected_stop_and_searches_with_location(
    test_data_directory: Path,
) -> list[StopAndSearch]:
    return get_data_from_file(
        test_data_directory,
        "stop_and_searches_with_location.json",
        get_stop_and_search,
    )


@pytest.fixture
def expected_stop_and_searches_without_location(
    test_data_directory: Path,
) -> list[StopAndSearch]:
    return get_data_from_file(
        test_data_directory,
        "stop_and_searches_without_location.json",
        get_stop_and_search,
    )


T = TypeVar("T", bound=SQLModel)


def get_data_from_file(
    test_data_directory: Path,
    file_name: str,
    mapping_func: Callable[[dict[str, Any]], T],
) -> list[T]:
    expected_data_path = test_data_directory / file_name
    with expected_data_path.open("r") as file:
        expected_data = json.load(file)
    return [mapping_func(data) for data in expected_data]


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
