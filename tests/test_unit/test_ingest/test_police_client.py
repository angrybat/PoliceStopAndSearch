from asyncio import gather
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import HTTPStatusError
from pytest import LogCaptureFixture

from src.ingest.police_client import BASE_URL, ONE_SECOND, PoliceClient
from src.models.bronze.available_date import AvailableDateWithForceIds
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch


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
        police_client.rate_limited_get = mock_get

        forces = await police_client.get_forces()

        assert forces == [
            Force(id="force1", name="Force One"),
            Force(id="force2", name="Force Two"),
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
        police_client.rate_limited_get = mock_get

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
        police_client.rate_limited_get = mock_get

        forces = await police_client.get_forces()

        assert forces == [Force(id="force2", name="Force Two")]
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            "Failed to map 'Force' at index '0' returned from Police API"
            in record.message
        )


class TestGetAvailableDates:
    @pytest.mark.asyncio
    async def test_returns_correct_available_dates(self):
        police_client = PoliceClient()
        returned_available_dates = [
            {"date": "2022-07", "stop-and-search": ["force-one", "force-two"]},
            {"date": "2023-07", "stop-and-search": ["force-one", "force-two"]},
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_available_dates
        mock_get = AsyncMock(return_value=mock_response)
        police_client.rate_limited_get = mock_get

        available_dates = await police_client.get_available_dates(
            datetime(2022, 6, 1), datetime(2023, 8, 1)
        )

        assert available_dates == [
            AvailableDateWithForceIds(
                **{"date": "2022-07", "stop-and-search": ["force-one", "force-two"]}
            ),
            AvailableDateWithForceIds(
                **{"date": "2023-07", "stop-and-search": ["force-one", "force-two"]},
            ),
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
        police_client.rate_limited_get = mock_get

        with pytest.raises(HTTPStatusError):
            await police_client.get_available_dates(
                datetime(2022, 6, 1), datetime(2023, 8, 1)
            )

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
        police_client.rate_limited_get = mock_get

        available_dates = await police_client.get_available_dates(
            datetime(2022, 6, 1), datetime(2023, 8, 1)
        )

        assert available_dates == [
            AvailableDateWithForceIds(
                **{"date": "2023-07", "stop-and-search": ["force-one", "force-two"]}
            )
        ]
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            "Failed to map 'AvailableDateWithForceIds' at index '0' returned from Police API"
            in record.message
        )


class TestGetStopAndSearches:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ["with_location", "route"],
        [(True, "stops-force"), (False, "stops-no-location")],
        ids=["with_location", "without_location"],
    )
    async def test_returns_correct_stop_and_searches(
        self, with_location: bool, route: str
    ):
        police_client = PoliceClient()
        returned_stop_and_searches = [
            {
                "age_range": "10-17",
                "outcome": "A no further action disposal",
                "involved_person": True,
                "self_defined_ethnicity": "Other ethnic group - Not stated",
                "gender": "Male",
                "legislation": "Firearms Act 1968 (section 47)",
                "outcome_linked_to_object_of_search": None,
                "datetime": "2023-07-31T15:37:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "outcome_object": {
                    "id": "bu-no-further-action",
                    "name": "A no further action disposal",
                },
                "location": None
                if not with_location
                else {
                    "latitude": 52.645734,
                    "street": {"id": 1735297, "name": "On or near Harrison Close"},
                    "longitude": -1.201507,
                },
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Firearms",
            },
            {
                "age_range": "18-24",
                "outcome": "Arrest",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Female",
                "legislation": "Misuse of Drugs Act 1971 (section 23)",
                "outcome_linked_to_object_of_search": True,
                "datetime": "2023-07-31T10:50:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "outcome_object": {"id": "bu-arrest", "name": "Arrest"},
                "location": None
                if not with_location
                else {
                    "latitude": 52.732829,
                    "street": {"id": 1740099, "name": "On or near St Gregory'S Drive"},
                    "longitude": -1.098693,
                },
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Controlled drugs",
            },
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_stop_and_searches
        mock_get = AsyncMock(return_value=mock_response)
        police_client.rate_limited_get = mock_get
        force_id = "leicestershire"
        date = "2023-07"

        stop_and_searches = await police_client.get_stop_and_searches(
            date, force_id, with_location
        )

        assert stop_and_searches == [
            StopAndSearch(
                id=None,
                force_id="leicestershire",
                age_range="10-17",
                involved_person=True,
                self_defined_ethnicity="Other ethnic group - Not stated",
                gender="Male",
                legislation="Firearms Act 1968 (section 47)",
                outcome_linked_to_object_of_search=None,
                datetime=datetime(2023, 7, 31, 15, 37, 00, tzinfo=UTC),
                removal_of_more_than_outer_clothing=False,
                outcome_id="bu-no-further-action",
                outcome_name="A no further action disposal",
                latitude=None if not with_location else Decimal("52.645734"),
                street_id=None if not with_location else 1735297,
                street_name=None if not with_location else "On or near Harrison Close",
                longitude=None if not with_location else Decimal("-1.201507"),
                operation=None,
                officer_defined_ethnicity="White",
                type="Person search",
                operation_name=None,
                object_of_search="Firearms",
            ),
            StopAndSearch(
                id=None,
                force_id="leicestershire",
                age_range="18-24",
                involved_person=True,
                self_defined_ethnicity="White - English/Welsh/Scottish/Northern Irish/British",
                gender="Female",
                legislation="Misuse of Drugs Act 1971 (section 23)",
                outcome_linked_to_object_of_search=True,
                datetime=datetime(2023, 7, 31, 10, 50, 00, tzinfo=UTC),
                removal_of_more_than_outer_clothing=False,
                outcome_id="bu-arrest",
                outcome_name="Arrest",
                latitude=None if not with_location else Decimal("52.732829"),
                street_id=None if not with_location else 1740099,
                street_name=None
                if not with_location
                else "On or near St Gregory'S Drive",
                longitude=None if not with_location else Decimal("-1.098693"),
                operation=None,
                officer_defined_ethnicity="White",
                type="Person search",
                operation_name=None,
                object_of_search="Controlled drugs",
            ),
        ]
        query_route = f"{route}?force={force_id}&date={date}"
        mock_get.assert_called_once_with(query_route)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ["with_location", "type"],
        [
            (True, "stop and searches with location"),
            (False, "stop and searches without location"),
        ],
        ids=["with_location", "without_location"],
    )
    async def test_logs_error_on_request_failure(
        self, with_location: bool, type: str, caplog: LogCaptureFixture
    ):
        police_client = PoliceClient()
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.side_effect = HTTPStatusError(
            "API says no", request=Mock(), response=Mock(status_code=500)
        )
        mock_get = AsyncMock(return_value=mock_response)
        police_client.rate_limited_get = mock_get
        force_id = "leicestershire"
        date = "2023-07"

        with pytest.raises(HTTPStatusError):
            await police_client.get_stop_and_searches(date, force_id, with_location)

        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            f"Failed to fetch {type} from Police API "
            f"for force with id '{force_id}' on date '{date}'" in record.message
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ["with_location", "type"],
        [
            (True, "stop and searches with location"),
            (False, "stop and searches without location"),
        ],
        ids=["with_location", "without_location"],
    )
    async def test_logs_error_on_mapping_failure(
        self, with_location: bool, type: str, caplog: LogCaptureFixture
    ):
        police_client = PoliceClient()
        returned_stop_and_searches = [
            {
                "not_age_range": "10-17",
                "not_outcome": "A no further action disposal",
                "not_involved_person": True,
                "not_self_defined_ethnicity": "Other ethnic group - Not stated",
                "not_gender": "Male",
                "not_legislation": "Firearms Act 1968 (section 47)",
                "not_outcome_linked_to_object_of_search": None,
                "not_datetime": "2023-07-31T15:37:00+00:00",
                "not_removal_of_more_than_outer_clothing": False,
                "not_outcome_object": {
                    "id": "bu-no-further-action",
                    "name": "A no further action disposal",
                },
                "not_location": None
                if not with_location
                else {
                    "latitude": 52.645734,
                    "street": {"id": 1735297, "name": "On or near Harrison Close"},
                    "longitude": -1.201507,
                },
                "not_operation": None,
                "not_officer_defined_ethnicity": "White",
                "not_type": "Person search",
                "not_operation_name": None,
                "not_object_of_search": "Firearms",
            },
            {
                "age_range": "18-24",
                "outcome": "Arrest",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Female",
                "legislation": "Misuse of Drugs Act 1971 (section 23)",
                "outcome_linked_to_object_of_search": True,
                "datetime": "2023-07-31T10:50:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "outcome_object": {"id": "bu-arrest", "name": "Arrest"},
                "location": None
                if not with_location
                else {
                    "latitude": 52.732829,
                    "street": {"id": 1740099, "name": "On or near St Gregory'S Drive"},
                    "longitude": -1.098693,
                },
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Controlled drugs",
            },
        ]
        mock_response = Mock()
        mock_response.json.return_value = returned_stop_and_searches
        mock_get = AsyncMock(return_value=mock_response)
        police_client.rate_limited_get = mock_get
        force_id = "leicestershire"
        date = "2023-07"

        stop_and_searches = await police_client.get_stop_and_searches(
            date, force_id, with_location
        )

        assert stop_and_searches == [
            StopAndSearch.model_validate(stop_and_search)
            for stop_and_search in returned_stop_and_searches[1:]
        ]
        record = caplog.records[-1]
        assert record.levelname == "ERROR"
        assert (
            "Failed to map 'StopAndSearch' at index '0' returned from Police API"
            in record.message
        )


class TestRateLimitedGet:
    @pytest.mark.asyncio
    async def test_get_method_is_called_correctly_and_response_returned(self):
        police_client = PoliceClient()
        mock_response = Mock()
        mock_get = AsyncMock(return_value=mock_response)
        police_client.get = mock_get
        route = "test_route"

        response = await police_client.rate_limited_get(route)

        mock_get.assert_called_once_with(route)
        assert response is mock_response

    @pytest.mark.asyncio
    async def test_requests_are_limited(self):
        calls_log = []
        requests_per_second = 5
        police_client = PoliceClient(max_requests_per_second=requests_per_second)
        mock_get = AsyncMock()
        police_client.get = mock_get
        mock_get.side_effect = lambda _: record_call_time(calls_log)
        calls = [police_client.rate_limited_get(f"test_route/{i}") for i in range(20)]

        await gather(*calls)

        calls_log.sort()
        for i in range(len(calls_log)):
            window_start = calls_log[i]
            count_in_window = sum(
                ONE_SECOND
                for time in calls_log
                if window_start <= time < window_start + 1
            )
            # Doubled requests per second because the API
            # can support twice the amount of requests in its bucket.
            assert count_in_window <= requests_per_second * 2, (
                f"Too many calls in 1 second: {count_in_window}"
            )


def record_call_time(call_log: list[float]):
    call_log.append(monotonic())
