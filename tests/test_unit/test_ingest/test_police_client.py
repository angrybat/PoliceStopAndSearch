from unittest.mock import AsyncMock, Mock

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture

from src.ingest.police_client import BASE_URL, PoliceClient
from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force


class TestInit:
    def test_initializes_with_default_base_url(self):
        police_client = PoliceClient()
        assert police_client.base_url == BASE_URL

    def test_initializes_with_custom_base_url(self):
        custom_url = "https://custom.police.api/"
        police_client = PoliceClient(base_url=custom_url)
        assert police_client.base_url == custom_url


class TestGetForces:
    @pytest.mark.asyncio
    async def test_returns_correct_forces(self):
        police_client = PoliceClient()
        returned_forces = [
            {"id": "force1", "name": "Force One"},
            {"id": "force2", "name": "Force Two"},
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_forces
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        forces = await police_client.get_forces()

        assert forces == [
            Force(id=None, name="Force One", api_id="force1"),
            Force(id=None, name="Force Two", api_id="force2"),
        ]
        mock_get.assert_called_once_with("forces")

    @pytest.mark.asyncio
    async def test_logs_error_on_request_failure(self, caplog: LogCaptureFixture):
        police_client = PoliceClient()
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.side_effect = HTTPStatusError(
            "API says no", request=Mock(), response=Mock(status_code=500)
        )
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        with pytest.raises(HTTPStatusError):
            await police_client.get_forces()

        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert "Failed to fetch forces from Police API" in record.message

    @pytest.mark.asyncio
    async def test_logs_error_on_mapping_failure(self, caplog: LogCaptureFixture):
        police_client = PoliceClient()
        returned_forces = [
            {"not_id": "force1", "not_name": "Force One"},
            {"id": "force2", "name": "Force Two"},
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_forces
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        with pytest.raises(KeyError):
            await police_client.get_forces()

        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert "Failed to map forces returned from Police API" in record.message


class TestGetAvailableDates:
    @pytest.mark.asyncio
    async def test_returns_correct_forces(self):
        police_client = PoliceClient()
        returned_available_dates = [
            {"date": "2022-07", "stop-and-search": ["force-one", "force-two"]},
            {"date": "2023-07", "stop-and-search": ["force-one", "force-two"]},
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_available_dates
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        forces = await police_client.get_available_dates()

        assert forces == [
            AvailableDate(id=None, year_month="2022-07"),
            AvailableDate(id=None, year_month="2023-07"),
        ]
        mock_get.assert_called_once_with("crimes-street-dates")

    @pytest.mark.asyncio
    async def test_logs_error_on_request_failure(self, caplog: LogCaptureFixture):
        police_client = PoliceClient()
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.side_effect = HTTPStatusError(
            "API says no", request=Mock(), response=Mock(status_code=500)
        )
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        with pytest.raises(HTTPStatusError):
            await police_client.get_available_dates()

        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert "Failed to fetch available dates from Police API" in record.message

    @pytest.mark.asyncio
    async def test_logs_error_on_mapping_failure(self, caplog: LogCaptureFixture):
        police_client = PoliceClient()
        returned_available_dates = [
            {"not_date": "2022-07", "not_stop-and-search": ["force-one", "force-two"]},
            {"date": "2023-07", "stop-and-search": ["force-one", "force-two"]},
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_available_dates
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get

        with pytest.raises(KeyError):
            await police_client.get_available_dates()

        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            "Failed to map available dates returned from Police API" in record.message
        )
