import json
from pathlib import Path

import pytest

from src.ingest.police_client import PoliceClient
from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force


@pytest.fixture
def test_data_directory() -> Path:
    return Path("tests/test_integration/test_police_api/test_data")


@pytest.fixture
def expected_forces(test_data_directory: Path) -> list[Force]:
    expected_forces_path = test_data_directory / "forces.json"
    with expected_forces_path.open("r") as file:
        expected_forces = json.load(file)
    return [Force(**force) for force in expected_forces]


@pytest.fixture
def expected_available_dates(test_data_directory: Path) -> list[AvailableDate]:
    expected_available_dates_path = test_data_directory / "available_dates.json"
    with expected_available_dates_path.open("r") as file:
        expected_available_dates = json.load(file)
    return [
        AvailableDate(id=None, year_month=available_date["date"])
        for available_date in expected_available_dates
    ]


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
