from collections.abc import Generator
from datetime import datetime
from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from police_api_ingester.models import AvailableDate, Force, StopAndSearch
from police_api_ingester.police_client import PoliceClient
from police_api_ingester.repositories import (
    StopAndSearchRepository,
)


@pytest.fixture
def mock_session() -> Generator[Session, None, None]:
    with patch.object(
        Session, "__enter__", new_callable=Mock(spec=Session)
    ) as mock_session_enter:
        mock_session = Mock()
        mock_session_enter.return_value = mock_session
        yield mock_session


@pytest.fixture
def stop_and_search_repository(
    mock_police_client: PoliceClient, mock_engine: Engine
) -> StopAndSearchRepository:
    return StopAndSearchRepository(mock_engine, mock_police_client)


class TestStoreStopAndSearches:
    @pytest.mark.asyncio
    async def test_makes_correct_store_stop_and_search_calls(
        self,
        stop_and_search_repository: StopAndSearchRepository,
    ):
        available_dates = [
            AvailableDate(
                year_month="2023-01",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
            AvailableDate(
                year_month="2023-02",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
        ]
        mock_available_date_repository = Mock()
        stop_and_search_repository.available_date_repository = (
            mock_available_date_repository
        )
        mock_get_available_dates = AsyncMock(return_value=available_dates)
        mock_available_date_repository.get_available_dates = mock_get_available_dates
        stop_and_search_repository.store_stop_and_search = AsyncMock()
        stop_and_search_repository.store_stop_and_search.side_effect = [
            True,
            True,
            True,
            True,
            True,
            True,
        ]
        from_datetime = datetime(2023, 1, 1, 1, 0, 0)
        to_datetime = datetime(2023, 2, 1, 3, 45, 0)

        success = await stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime
        )

        assert success is True
        mock_get_available_dates.assert_called_once_with(
            from_datetime, to_datetime, with_forces=True
        )
        stop_and_search_repository.store_stop_and_search.assert_has_awaits(
            [
                call("2023-01", "force-one", from_datetime, to_datetime),
                call("2023-01", "force-two", from_datetime, to_datetime),
                call("2023-01", "force-three", from_datetime, to_datetime),
                call("2023-02", "force-one", from_datetime, to_datetime),
                call("2023-02", "force-two", from_datetime, to_datetime),
                call("2023-02", "force-three", from_datetime, to_datetime),
            ],
            any_order=True,
        )

    @pytest.mark.asyncio
    async def test_calls_store_available_dates_with_correct_parameters(
        self,
        stop_and_search_repository: StopAndSearchRepository,
    ):
        available_dates = [
            AvailableDate(
                year_month="2023-01",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
            AvailableDate(
                year_month="2023-02",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
        ]
        mock_available_date_repository = AsyncMock()
        stop_and_search_repository.available_date_repository = (
            mock_available_date_repository
        )
        mock_get_available_dates = AsyncMock(return_value=available_dates)
        mock_available_date_repository.get_available_dates = mock_get_available_dates
        stop_and_search_repository.store_stop_and_search = AsyncMock()
        stop_and_search_repository.store_stop_and_search.side_effect = [
            True,
            True,
            True,
            True,
            True,
            True,
        ]
        from_datetime = datetime(2023, 1, 1, 1, 0, 0)
        to_datetime = datetime(2023, 2, 1, 3, 45, 0)

        success = await stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, True, ["force-1"]
        )

        assert success is True
        mock_available_date_repository.store_available_dates.assert_called_once_with(
            from_datetime, to_datetime, ["force-1"]
        )

    @pytest.mark.asyncio
    async def test_returns_false_if_available_dates_is_not_stored(
        self,
        stop_and_search_repository: StopAndSearchRepository,
    ):
        mock_available_date_repository = AsyncMock()
        stop_and_search_repository.available_date_repository = (
            mock_available_date_repository
        )
        mock_store_available_dates = AsyncMock(return_value=False)
        mock_available_date_repository.store_available_dates = (
            mock_store_available_dates
        )
        from_datetime = datetime(2023, 1, 1, 1, 0, 0)
        to_datetime = datetime(2023, 2, 1, 3, 45, 0)

        success = await stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, True, ["force-1"]
        )

        assert success is False
        mock_available_date_repository.store_available_dates.assert_called_once_with(
            from_datetime, to_datetime, ["force-1"]
        )

    @pytest.mark.asyncio
    async def test_returns_false_if_a_stop_and_search_calls_fails(
        self,
        stop_and_search_repository: StopAndSearchRepository,
    ):
        available_dates = [
            AvailableDate(
                year_month="2023-01",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
            AvailableDate(
                year_month="2023-02",
                forces=[
                    Force(id="force-one", name="Force One"),
                    Force(id="force-two", name="Force Two"),
                    Force(id="force-three", name="Force Three"),
                ],
            ),
        ]
        mock_available_date_repository = Mock()
        stop_and_search_repository.available_date_repository = (
            mock_available_date_repository
        )
        mock_get_available_dates = AsyncMock(return_value=available_dates)
        mock_available_date_repository.get_available_dates = mock_get_available_dates
        stop_and_search_repository.store_stop_and_search = AsyncMock()
        stop_and_search_repository.store_stop_and_search.side_effect = [
            True,
            True,
            True,
            True,
            False,
            True,
        ]
        from_datetime = datetime(2023, 1, 1, 1, 0, 0)
        to_datetime = datetime(2023, 2, 1, 3, 45, 0)

        success = await stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime
        )

        assert success is False

    @pytest.mark.asyncio
    async def test_returns_false_when_cannot_retrieve_available_dates(
        self,
        stop_and_search_repository: StopAndSearchRepository,
    ):
        mock_available_date_repository = Mock()
        stop_and_search_repository.available_date_repository = (
            mock_available_date_repository
        )
        mock_get_available_dates = AsyncMock(return_value=None)
        mock_available_date_repository.get_available_dates = mock_get_available_dates
        from_datetime = datetime(2023, 1, 1, 1, 0, 0)
        to_datetime = datetime(2023, 2, 1, 3, 45, 0)

        success = await stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime
        )

        assert success is False


class TestStopAndSearch:
    @pytest.mark.asyncio
    async def test_stop_and_searches_added_to_session_and_commited(
        self,
        mock_session: Mock,
        stop_and_search_repository: StopAndSearchRepository,
        mock_police_client: PoliceClient,
    ):
        stop_and_searches_with_location = [
            get_mock_stop_and_search(datetime(2023, 1, 1)),
            get_mock_stop_and_search(datetime(2023, 1, 2)),
            get_mock_stop_and_search(datetime(2023, 1, 3)),
        ]
        stop_and_searches_without_location = [
            get_mock_stop_and_search(datetime(2023, 1, 4)),
            get_mock_stop_and_search(datetime(2023, 1, 5)),
            get_mock_stop_and_search(datetime(2023, 1, 6)),
        ]
        mock_police_client.get_stop_and_searches.side_effect = [
            stop_and_searches_with_location,
            stop_and_searches_without_location,
        ]
        year_month = "2023-01"
        force_id = "force-one"

        success = await stop_and_search_repository.store_stop_and_search(
            year_month, force_id, datetime(2023, 1, 2), datetime(2023, 1, 5)
        )

        all_stop_and_searches = (
            stop_and_searches_with_location + stop_and_searches_without_location
        )
        assert success is True
        mock_session.add_all.assert_called_once_with(all_stop_and_searches[1:-1])
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_police_client_stop_and_searches_called_correctly(
        self,
        mock_session: Mock,
        stop_and_search_repository: StopAndSearchRepository,
        mock_police_client: PoliceClient,
    ):
        stop_and_searches_with_location = [
            get_mock_stop_and_search(datetime(2023, 1, 1)),
            get_mock_stop_and_search(datetime(2023, 1, 2)),
            get_mock_stop_and_search(datetime(2023, 1, 3)),
        ]
        stop_and_searches_without_location = [
            get_mock_stop_and_search(datetime(2023, 1, 4)),
            get_mock_stop_and_search(datetime(2023, 1, 5)),
            get_mock_stop_and_search(datetime(2023, 1, 6)),
        ]
        mock_police_client.get_stop_and_searches.side_effect = [
            stop_and_searches_with_location,
            stop_and_searches_without_location,
        ]
        year_month = "2023-01"
        force_id = "force-one"

        success = await stop_and_search_repository.store_stop_and_search(
            year_month, force_id, datetime(2023, 1, 2), datetime(2023, 1, 5)
        )

        assert success is True
        mock_police_client.get_stop_and_searches.assert_has_awaits(
            [
                call(year_month, force_id, with_location=True),
                call(year_month, force_id, with_location=False),
            ],
            any_order=True,
        )

    @pytest.mark.asyncio
    async def test_return_falses_if_police_client_has_http_status_error(
        self,
        mock_session: Mock,
        stop_and_search_repository: StopAndSearchRepository,
        mock_police_client: PoliceClient,
    ):
        mock_police_client.get_stop_and_searches.side_effect = HTTPStatusError(
            "cannot contact the police", request=Mock(), response=Mock()
        )
        year_month = "2023-01"
        force_id = "force-one"

        success = await stop_and_search_repository.store_stop_and_search(
            year_month, force_id, datetime(2023, 1, 2), datetime(2023, 1, 5)
        )

        assert success is False

    @pytest.mark.asyncio
    async def test_logs_error_if_cannot_store_stop_and_searches(
        self,
        mock_session: Mock,
        stop_and_search_repository: StopAndSearchRepository,
        mock_police_client: PoliceClient,
        caplog: LogCaptureFixture,
    ):
        stop_and_searches_with_location = [
            get_mock_stop_and_search(datetime(2023, 1, 1)),
            get_mock_stop_and_search(datetime(2023, 1, 2)),
            get_mock_stop_and_search(datetime(2023, 1, 3)),
        ]
        stop_and_searches_without_location = [
            get_mock_stop_and_search(datetime(2023, 1, 4)),
            get_mock_stop_and_search(datetime(2023, 1, 5)),
            get_mock_stop_and_search(datetime(2023, 1, 6)),
        ]
        mock_police_client.get_stop_and_searches.side_effect = [
            stop_and_searches_with_location,
            stop_and_searches_without_location,
        ]
        year_month = "2023-01"
        force_id = "force-one"
        mock_session.commit.side_effect = SQLAlchemyError("database cannot store")

        success = await stop_and_search_repository.store_stop_and_search(
            year_month, force_id, datetime(2023, 1, 2), datetime(2023, 1, 5)
        )

        assert success is False
        record = caplog.records[-1]
        assert (
            record.message
            == "Cannot store StopAndSearches in the database for 'force-one' on date '2023-01'."
        )
        assert record.levelname == "WARNING"


def get_mock_stop_and_search(datetime: datetime) -> StopAndSearch:
    mock = Mock(spec=StopAndSearch)
    mock.datetime = datetime
    return mock
