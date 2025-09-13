from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture, Session
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.police_api_ingester.models import Force
from src.police_api_ingester.police_client import PoliceClient
from src.police_api_ingester.repositories import ForceRepository


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
        mock_session.exec.return_value.all.return_value = []

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
    async def test_stores_only_force_not_in_database(
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
        force_repository.get_all_forces = AsyncMock()
        force_repository.get_all_forces.return_value = [
            Force(id="force-1", name="Force One")
        ]

        stored_forces = await force_repository.store_forces()

        assert stored_forces == forces
        mock_session.add_all.assert_called_once_with(forces[1:])
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_has_calls(
            [
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
        force_repository.get_all_forces = AsyncMock()
        force_repository.get_all_forces.return_value = [
            Force(id="force-1", name="Force One")
        ]

        stored_forces = await force_repository.store_forces()

        assert stored_forces is None

    @pytest.mark.asyncio
    async def test_return_none_when_cannot_get_forces_from_database(
        self,
        force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        mock_police_client.get_forces.return_value = forces
        force_repository.get_all_forces = AsyncMock()
        force_repository.get_all_forces.return_value = None

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
        mock_session.exec.return_value.all.return_value = []
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
        mock_session.exec.return_value.all.return_value = []
        mock_session.refresh.side_effect = SQLAlchemyError(
            "database not accepting forces"
        )

        stored_forces = await force_repository.store_forces()

        assert stored_forces is None
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert record.message == "Could not refresh Forces in the database."


class TestGetAllForces:
    @pytest.mark.asyncio
    @patch("src.police_api_ingester.repositories.force_repository.select")
    async def test_correct_query_is_created(
        self,
        mock_select: Mock,
        mock_session: Session,
        force_repository: ForceRepository,
    ):
        stored_forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]

        mock_session.exec.return_value.all.return_value = stored_forces

        forces = await force_repository.get_all_forces()

        assert forces == stored_forces
        mock_select.assert_called_once_with(Force)

    @pytest.mark.asyncio
    async def test_returns_none_when_exception_is_thrown(
        self,
        mock_session: Session,
        force_repository: ForceRepository,
        caplog: LogCaptureFixture,
    ):
        mock_session.exec.side_effect = SQLAlchemyError("oh not can't get forces!")

        forces = await force_repository.get_all_forces()

        assert forces is None
        record = caplog.records[-1]
        assert record.message == "Cannot get existing Forces from the database."
        assert record.levelname == "ERROR"
