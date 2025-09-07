from unittest.mock import AsyncMock, Mock

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture

from src.ingest.police_client import PoliceClient
from src.models.bronze.force import Force


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
