from datetime import datetime
from http import HTTPStatus
from logging import Logger, getLogger
from typing import TypeVar

from aiolimiter import AsyncLimiter
from httpx import AsyncClient, HTTPStatusError, ReadTimeout, Response, Timeout
from httpx._client import DEFAULT_TIMEOUT_CONFIG
from pydantic_core import ValidationError
from sqlmodel import SQLModel

from police_api_ingester.models import (
    AvailableDateWithForceIds,
    Force,
    StopAndSearch,
)

BASE_URL = "https://data.police.uk/api/"

T = TypeVar("T", bound=SQLModel)

ONE_SECOND = 1


class PoliceClient(AsyncClient):
    def __init__(
        self,
        base_url: str = BASE_URL,
        timeout: Timeout = DEFAULT_TIMEOUT_CONFIG,
        logger: Logger | None = None,
        max_requests_per_second: int = 15,
        max_request_retries: int = 5,
    ):
        self.logger = logger or getLogger("PoliceClient")
        self.limiter = AsyncLimiter(1, ONE_SECOND / max_requests_per_second)
        self.max_request_retries = max_request_retries
        super().__init__(base_url=base_url, timeout=timeout)

    async def get_forces(self, force_ids: list[str] | None = None) -> list[Force]:
        forces = await self._get_response_body(
            "forces", "Failed to fetch forces from Police API"
        )
        if force_ids is None:
            return self._map_vailidate_models(Force, forces)
        return [
            force
            for force in self._map_vailidate_models(Force, forces)
            if force.id in force_ids
        ]

    async def get_available_dates(
        self, from_date: datetime, to_date: datetime
    ) -> list[AvailableDateWithForceIds]:
        available_dates = await self._get_response_body(
            "crimes-street-dates", "Failed to fetch available dates from Police API"
        )
        from_year_month = from_date.strftime("%Y-%m")
        to_year_month = to_date.strftime("%Y-%m")
        return [
            date
            for date in self._map_vailidate_models(
                AvailableDateWithForceIds, available_dates
            )
            if from_year_month <= date.year_month and date.year_month <= to_year_month
        ]

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
        for stop_and_search in stop_and_searches:
            stop_and_search["force_id"] = force_id
        return self._map_vailidate_models(StopAndSearch, stop_and_searches)

    async def rate_limited_get(self, route: str) -> Response:
        attempts = 0
        while attempts < self.max_request_retries:
            attempts += 1
            async with self.limiter:
                try:
                    response = await self.get(route)
                except ReadTimeout:
                    self.logger.warning(
                        "The API caused a read time out."
                        f"Currently at attempt '{attempts}' of '{self.max_request_retries}'."
                        " Retrying..."
                    )
                    continue
            if response.status_code != HTTPStatus.TOO_MANY_REQUESTS:
                return response
            self.logger.warning(
                "The rate limit on the API has been exceeded. "
                f"Currently at attempt '{attempts}' of '{self.max_request_retries}'."
                " Retrying..."
            )
        self.logger.exception(
            "The rate limit on the API has been exceeded. The max limit of retires "
            f"'{self.max_request_retries}' has been exceeded, giving up."
        )
        response.raise_for_status()
        return response

    async def _get_response_body(self, route: str, error_message: str) -> list[dict]:
        response = await self.rate_limited_get(route)
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
