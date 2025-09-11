from logging import Logger, getLogger
from typing import TypeVar

from httpx import AsyncClient, HTTPStatusError
from pydantic_core import ValidationError
from sqlmodel import SQLModel

from src.models.bronze.available_date import AvailableDateWithForceIds
from src.models.bronze.force import Force
from src.models.bronze.stop_and_search import StopAndSearch

BASE_URL = "https://data.police.uk/api/"

T = TypeVar("T", bound=SQLModel)


class PoliceClient(AsyncClient):
    def __init__(self, base_url: str = BASE_URL, logger: Logger | None = None):
        self.logger = logger or getLogger("PoliceClient")
        super().__init__(base_url=base_url)

    async def get_forces(self) -> list[Force]:
        forces = await self._get_response_body(
            "forces", "Failed to fetch forces from Police API"
        )
        return self._map_vailidate_models(Force, forces)

    async def get_available_dates(self) -> list[AvailableDateWithForceIds]:
        available_dates = await self._get_response_body(
            "crimes-street-dates", "Failed to fetch available dates from Police API"
        )
        return self._map_vailidate_models(AvailableDateWithForceIds, available_dates)

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
        return self._map_vailidate_models(StopAndSearch, stop_and_searches)

    async def _get_response_body(self, endpoint: str, error_message: str) -> list[dict]:
        response = await self.get(endpoint)
        try:
            response.raise_for_status()
        except HTTPStatusError as error:
            self.logger.exception(error_message)
            raise error
        return response.json()

    def _map_vailidate_models(self, model: type[T], data: list[dict]) -> list[T]:
        models = []
        for index, dict in enumerate(data):
            try:
                models.append(model.model_validate(dict))
            except ValidationError:
                self.logger.exception(
                    f"Failed to map '{model.__name__}' at index '{index}' "
                    "returned from Police API"
                )
                continue
        return models
