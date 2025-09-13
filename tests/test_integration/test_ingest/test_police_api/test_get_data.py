from datetime import UTC, datetime

import pytest

from src.police_api_ingester.models import (
    AvailableDateWithForceIds,
    Force,
    StopAndSearch,
)
from src.police_api_ingester.police_client import (
    PoliceClient,
)


class TestGetForces:
    @pytest.mark.asyncio
    async def test_get_forces(self, expected_forces_without_btp: list[Force]):
        police_client = PoliceClient()

        forces = await police_client.get_forces()

        assert forces == expected_forces_without_btp


class TestGetAvailableDates:
    @pytest.mark.asyncio
    async def test_get_available_dates(
        self, expected_available_dates: list[AvailableDateWithForceIds]
    ):
        police_client = PoliceClient()

        available_dates = await police_client.get_available_dates(
            datetime(2023, 4, 1, tzinfo=UTC), datetime(2023, 6, 1, tzinfo=UTC)
        )

        assert available_dates == expected_available_dates


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
