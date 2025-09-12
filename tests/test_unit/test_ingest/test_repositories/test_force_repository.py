from unittest.mock import Mock, call

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.ingest.police_client import PoliceClient
from src.ingest.repositories.force_repository import ForceRepository
from src.models.bronze.force import Force


@pytest.fixture
def force_repository(
    mock_engine: Engine, mock_police_client: PoliceClient
) -> ForceRepository:
    return ForceRepository(mock_engine, mock_police_client)


class TestStoreForces:
    @pytest.mark.asyncio
    async def test_returns_forces_that_are_stored(
        self,
        mock_session: Mock,
        force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        mock_police_client.get_forces.return_value = forces

        stored_forces = await force_repository.store_forces()

        assert stored_forces == forces
        mock_session.add_all.assert_called_once_with(forces)
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_has_calls(
            [
                call(Force(id="force-1", name="Force One")),
                call(Force(id="force-2", name="Force Two")),
                call(Force(id="force-3", name="Force Three")),
            ]
        )

    @pytest.mark.asyncio
    async def test_return_none_when_get_forces_throw_http_status_error(
        self,
        force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        mock_police_client.get_forces.side_effect = HTTPStatusError(
            "you get no forces!", response=Mock(), request=Mock()
        )

        stored_forces = await force_repository.store_forces()

        assert stored_forces is None

    @pytest.mark.asyncio
    async def test_logs_error_when_cannot_store_forces_in_database(
        self,
        mock_session: Mock,
        force_repository: ForceRepository,
        mock_police_client: PoliceClient,
        caplog: LogCaptureFixture,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        mock_police_client.get_forces.return_value = forces
        mock_session.commit.side_effect = SQLAlchemyError(
            "database not accepting forces"
        )

        stored_forces = await force_repository.store_forces()

        assert stored_forces is None
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert record.message == "Could not store Forces in the database."

    @pytest.mark.asyncio
    async def test_logs_error_when_cannot_refresh_forces_in_database(
        self,
        mock_session: Mock,
        force_repository: ForceRepository,
        mock_police_client: PoliceClient,
        caplog: LogCaptureFixture,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        mock_police_client.get_forces.return_value = forces
        mock_session.refresh.side_effect = SQLAlchemyError(
            "database not accepting forces"
        )

        stored_forces = await force_repository.store_forces()

        assert stored_forces is None
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert record.message == "Could not refresh Forces in the database."
