from datetime import datetime
from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.operators import ge, le
from sqlmodel import Session

from src.ingest.police_client import PoliceClient
from src.ingest.repositories.available_date_repository import AvailableDateRepository
from src.ingest.repositories.force_repository import ForceRepository
from src.models.bronze.available_date import AvailableDate, AvailableDateWithForceIds
from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping
from src.models.bronze.force import Force


@pytest.fixture
def mock_force_repository() -> ForceRepository:
    return Mock(spec=ForceRepository)


@pytest.fixture
def available_date_repository(
    mock_engine: Engine, mock_police_client: PoliceClient, mock_force_repository
) -> AvailableDateRepository:
    repository = AvailableDateRepository(mock_engine, mock_police_client)
    repository.force_repository = mock_force_repository
    return repository


class TestStoreAvailableDates:
    @pytest.mark.asyncio
    async def test_calls_store_available_date_on_dates_returned_from_the_api(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        available_dates = [
            AvailableDateWithForceIds(
                **{"date": "2023-02", "stop-and-search": ["force-1"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-03", "stop-and-search": ["force-2"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-04", "stop-and-search": ["force-3"]}
            ),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.return_value = available_dates
        available_date_repository.store_available_date = AsyncMock()
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        available_date_repository.store_available_date.return_value = True
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is True
        available_date_repository.store_available_date.assert_has_awaits(
            [
                call(
                    AvailableDateWithForceIds(
                        **{"date": "2023-02", "stop-and-search": ["force-1"]}
                    ),
                    [],
                ),
                call(
                    AvailableDateWithForceIds(
                        **{"date": "2023-03", "stop-and-search": ["force-2"]}
                    ),
                    [],
                ),
                call(
                    AvailableDateWithForceIds(
                        **{"date": "2023-04", "stop-and-search": ["force-3"]}
                    ),
                    [],
                ),
            ],
            any_order=True,
        )
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_stores_forces_not_returned_from_force_repository(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        available_dates = [
            AvailableDateWithForceIds(
                **{"date": "2023-02", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-03", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-04", "stop-and-search": ["force-3"]}
            ),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.return_value = available_dates
        available_date_repository.store_available_date = AsyncMock()
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is True
        mock_session.commit.assert_called_once()
        mock_session.add_all.assert_called_once_with({Force(id="force-4")})

    @pytest.mark.asyncio
    async def test_returns_false_if_no_forces_are_stored(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = None
        available_dates = [
            AvailableDateWithForceIds(
                **{"date": "2023-02", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-03", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-04", "stop-and-search": ["force-3"]}
            ),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.return_value = available_dates
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        available_date_repository.store_available_date = AsyncMock()
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is False

    @pytest.mark.asyncio
    async def test_returns_false_if_cannot_get_available_dates_from_api(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.side_effect = HTTPStatusError(
            "cannot get available dates", request=Mock(), response=Mock()
        )
        available_date_repository.store_available_date = AsyncMock()
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is False

    @pytest.mark.asyncio
    async def test_logs_error_when_cannot_store_missing_forces_to_database(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
        caplog: LogCaptureFixture,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        available_dates = [
            AvailableDateWithForceIds(
                **{"date": "2023-02", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-03", "stop-and-search": ["force-4"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-04", "stop-and-search": ["force-3"]}
            ),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.return_value = available_dates
        available_date_repository.store_available_date = AsyncMock()
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)
        mock_session.commit.side_effect = SQLAlchemyError(
            "database not storing available dates"
        )

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is False
        record = caplog.records[-1]
        assert record.message == "Cannot store missing Forces in the database."
        assert record.levelname == "ERROR"

    @pytest.mark.asyncio
    async def test_returns_false_if_a_single_store_available_date_fails(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        mock_force_repository: ForceRepository,
        mock_police_client: PoliceClient,
    ):
        forces = [
            Force(id="force-1", name="Force One"),
            Force(id="force-2", name="Force Two"),
            Force(id="force-3", name="Force Three"),
        ]
        available_dates = [
            AvailableDateWithForceIds(
                **{"date": "2023-02", "stop-and-search": ["force-1"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-03", "stop-and-search": ["force-2"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-04", "stop-and-search": ["force-3"]}
            ),
        ]
        mock_force_repository.store_forces.return_value = forces
        mock_police_client.get_available_dates.return_value = available_dates
        available_date_repository.store_available_date = AsyncMock()
        available_date_repository.get_available_dates = AsyncMock()
        available_date_repository.get_available_dates.return_value = []
        available_date_repository.store_available_date.side_effect = [True, False, True]
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 5, 1)

        success = await available_date_repository.store_available_dates(
            from_date, to_date
        )

        assert success is False


class TestStoreAvailableDate:
    @pytest.mark.asyncio
    async def test_stores_available_date_and_mappings(
        self, mock_session: Session, available_date_repository: AvailableDateRepository
    ):
        year_month = "2023-01"
        force_ids = ["force-1", "force-2", "force-3"]
        available_date_with_force_ids = AvailableDateWithForceIds(
            **{"date": year_month, "stop-and-search": force_ids}
        )
        mock_session.add.side_effect = set_id

        success = await available_date_repository.store_available_date(
            available_date_with_force_ids, []
        )

        assert success is True
        mock_session.add.assert_called_once_with(
            AvailableDate(year_month=year_month, id=1)
        )
        mock_session.add_all.assert_called_once_with(
            [
                AvailableDateForceMapping(available_date_id=1, force_id="force-1"),
                AvailableDateForceMapping(available_date_id=1, force_id="force-2"),
                AvailableDateForceMapping(available_date_id=1, force_id="force-3"),
            ],
        )
        mock_session.commit.assert_called_once()
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_stores_available_date_and_mappings_that_dont_exist(
        self, mock_session: Session, available_date_repository: AvailableDateRepository
    ):
        year_month = "2023-01"
        force_ids = ["force-1", "force-2", "force-3"]
        available_date_with_force_ids = AvailableDateWithForceIds(
            **{"date": year_month, "stop-and-search": force_ids}
        )
        mock_session.add.side_effect = set_id

        success = await available_date_repository.store_available_date(
            available_date_with_force_ids,
            [
                AvailableDate(
                    id=1,
                    year_month=year_month,
                    forces=[Force(id="force-1", name="Force One")],
                )
            ],
        )

        assert success is True
        mock_session.add_all.assert_called_once_with(
            [
                AvailableDateForceMapping(available_date_id=1, force_id="force-2"),
                AvailableDateForceMapping(available_date_id=1, force_id="force-3"),
            ],
        )
        mock_session.commit.assert_called_once()
        mock_session.flush.assert_not_called()
        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_logs_warning_when_cannot_flush_available_date(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        caplog: LogCaptureFixture,
    ):
        year_month = "2023-01"
        force_ids = ["force-1", "force-2", "force-3"]
        available_date_with_force_ids = AvailableDateWithForceIds(
            **{"date": year_month, "stop-and-search": force_ids}
        )
        mock_session.flush.side_effect = SQLAlchemyError("Oh No!")

        success = await available_date_repository.store_available_date(
            available_date_with_force_ids, []
        )

        assert success is False
        record = caplog.records[-1]
        assert record.levelname == "WARNING"
        assert (
            record.message
            == "Cannot flush AvailableDate to the database for date '2023-01'."
        )

    @pytest.mark.asyncio
    async def test_logs_warning_when_cannot_commit_available_date_and_mappings(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        caplog: LogCaptureFixture,
    ):
        year_month = "2023-01"
        force_ids = ["force-1", "force-2", "force-3"]
        available_date_with_force_ids = AvailableDateWithForceIds(
            **{"date": year_month, "stop-and-search": force_ids}
        )
        mock_session.commit.side_effect = SQLAlchemyError("Oh No!")

        success = await available_date_repository.store_available_date(
            available_date_with_force_ids, []
        )

        assert success is False
        record = caplog.records[-1]
        assert record.levelname == "WARNING"
        assert (
            record.message
            == "Cannot store AvailableDate and AvailableDateForceMappings in the database for date '2023-01'."
        )


def set_id(date: AvailableDate):
    if isinstance(date, AvailableDate):
        date.id = 1


class TestGetAvailableDates:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "with_forces", [True, False], ids=["with_forces=True", "with_forces=False"]
    )
    @patch("src.ingest.repositories.available_date_repository.joinedload")
    @patch("src.ingest.repositories.available_date_repository.and_")
    @patch("src.ingest.repositories.available_date_repository.select")
    async def test_creates_correct_query_and_returns_correct_dates(
        self,
        mock_select: Mock,
        mock_and_: Mock,
        mock_joinedload: Mock,
        with_forces: bool,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
    ):
        mock_available_dates = [Mock(), Mock(), Mock()]
        mock_session.exec.return_value.unique.return_value.all.return_value = (
            mock_available_dates
        )
        mock_options = Mock()
        mock_where = Mock()
        mock_select.return_value.where = mock_where
        mock_where.return_value.options = mock_options
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 4, 1)

        available_dates = await available_date_repository.get_available_dates(
            from_date, to_date, with_forces=with_forces
        )

        assert available_dates == mock_available_dates
        mock_select.assert_called_once_with(AvailableDate)
        mock_where.assert_called_once_with(mock_and_.return_value)
        from_clause, to_clause = mock_and_.call_args_list[0][0]
        mock_and_.assert_called_once_with(from_clause, to_clause)
        assert from_clause.left == AvailableDate.year_month
        assert from_clause.right.value == "2023-01"
        assert from_clause.operator == ge
        assert to_clause.right.value == "2023-04"
        assert to_clause.left == AvailableDate.year_month
        assert to_clause.operator == le
        if with_forces:
            mock_options.assert_called_once_with(mock_joinedload.return_value)
            mock_joinedload.assert_called_once_with(AvailableDate.forces)
        else:
            mock_options.assert_not_called()
            mock_joinedload.assert_not_called()

    @pytest.mark.asyncio
    async def test_logs_error_when_cannot_get_dates(
        self,
        mock_session: Session,
        available_date_repository: AvailableDateRepository,
        caplog: LogCaptureFixture,
    ):
        mock_session.exec.side_effect = SQLAlchemyError("Database says no!")
        from_date = datetime(2023, 1, 1)
        to_date = datetime(2023, 4, 1)

        available_dates = await available_date_repository.get_available_dates(
            from_date, to_date, with_forces=True
        )

        assert available_dates is None
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            record.message == "Could not retrieve AvailableDates from the "
            "database between '2023-01' to '2023-04'."
        )
