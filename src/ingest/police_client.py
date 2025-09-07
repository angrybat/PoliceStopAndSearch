from logging import Logger, getLogger
from typing import Any

from httpx import AsyncClient, HTTPStatusError

from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch

BASE_URL = "https://data.police.uk/api/"


class PoliceClient(AsyncClient):
    def __init__(self, base_url: str = BASE_URL, logger: Logger | None = None):
        self.logger = logger or getLogger("PoliceClient")
        super().__init__(base_url=base_url)

    async def get_forces(self) -> list[Force]:
        forces = await self._get_response_body(
            "forces", "Failed to fetch forces from Police API"
        )
        try:
            return [get_force(force) for force in forces]
        except KeyError as error:
            self.logger.exception("Failed to map forces returned from Police API")
            raise error

    async def get_available_dates(self) -> list[AvailableDate]:
        available_dates = await self._get_response_body(
            "crimes-street-dates", "Failed to fetch available dates from Police API"
        )
        try:
            return [get_available_date(date) for date in available_dates]
        except KeyError as error:
            self.logger.exception(
                "Failed to map available dates returned from Police API"
            )
            raise error

    async def get_stop_and_searches(
        self, date: str, force_id: str, with_location: bool
    ) -> list[StopAndSearch]:
        endpoint = (
            f"stops-force?force={force_id}&date={date}"
            if with_location
            else f"stops-no-location?force={force_id}&date={date}"
        )
        error_message = (
            f"Failed to fetch stop and searches with "
            f"location from Police API for force with id '{force_id}' on date '{date}'"
            if with_location
            else f"Failed to fetch stop and searches without "
            f"location from Police API for force with id '{force_id}' on date '{date}'"
        )
        stop_and_searches = await self._get_response_body(endpoint, error_message)
        try:
            return [
                get_stop_and_search(stop_and_search)
                for stop_and_search in stop_and_searches
            ]
        except KeyError as error:
            error_message = (
                f"Failed to map stop and searches with location"
                f" from Police API for force with id '{force_id}' on date '{date}'"
                if with_location
                else f"Failed to map stop and searches without location"
                f" from Police API for force with id '{force_id}' on date '{date}'"
            )
            self.logger.exception(error_message)
            raise error

    async def _get_response_body(self, endpoint: str, error_message: str) -> list[dict]:
        response = await self.get(endpoint)
        try:
            response.raise_for_status()
        except HTTPStatusError as error:
            self.logger.exception(error_message)
            raise error
        return response.json()


def get_force(force: dict[str, Any]) -> Force:
    return Force(id=None, name=force["name"], api_id=force["id"])


def get_available_date(available_date: dict[str, Any]) -> AvailableDate:
    return AvailableDate(id=None, year_month=available_date["date"])


def get_stop_and_search(
    stop_and_search: dict[str, Any],
) -> StopAndSearch:
    latitude = get_location_property(stop_and_search, "latitude")
    longitude = get_location_property(stop_and_search, "longitude")
    street_id = get_street_property(stop_and_search, "id")
    street_name = get_street_property(stop_and_search, "name")
    outcome_name = get_outcome_property(stop_and_search, "name")
    outcome_id = get_outcome_property(stop_and_search, "id")
    return StopAndSearch(
        force_id=None,
        id=None,
        type=stop_and_search["type"],
        involved_person=stop_and_search["involved_person"],
        datetime=stop_and_search.get("datetime"),
        operation=stop_and_search.get("operation", None),
        operation_name=stop_and_search.get("operation_name", None),
        latitude=latitude,
        longitude=longitude,
        street_id=street_id,
        street_name=street_name,
        gender=stop_and_search.get("gender", None),
        age_range=stop_and_search["age_range"],
        self_defined_ethnicity=stop_and_search["self_defined_ethnicity"],
        officer_defined_ethnicity=stop_and_search["officer_defined_ethnicity"],
        legislation=stop_and_search["legislation"],
        object_of_search=stop_and_search["object_of_search"],
        outcome_name=outcome_name,
        outcome_id=outcome_id,
        outcome_linked_to_object_of_search=stop_and_search.get(
            "outcome_linked_to_object_of_search", None
        ),
        removal_of_more_than_outer_clothing=stop_and_search.get(
            "removal_of_more_than_outer_clothing", None
        ),
    )


def get_child_property(object: dict[str, Any], parent: str, key: str) -> Any | None:
    try:
        child = object[parent]
        if child is None:
            return None
        return child[key]
    except KeyError:
        return None


def get_location_property(stop_and_search: dict[str, Any], key: str) -> Any | None:
    return get_child_property(stop_and_search, "location", key)


def get_street_property(stop_and_search: dict[str, Any], key: str) -> Any | None:
    try:
        street = get_location_property(stop_and_search, "street")
        if street is None:
            return None
        return street[key]
    except KeyError:
        return None


def get_outcome_property(stop_and_search: dict[str, Any], key: str) -> Any | None:
    return get_child_property(stop_and_search, "outcome_object", key)
