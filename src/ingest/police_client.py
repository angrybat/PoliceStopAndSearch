import logging

from httpx import AsyncClient, HTTPStatusError

from src.models.bronze.available_date import AvailableDate
from src.models.bronze.force import Force

BASE_URL = "https://data.police.uk/api/"


class PoliceClient(AsyncClient):
    def __init__(self, base_url: str = BASE_URL):
        super().__init__(base_url=base_url)

    async def get_forces(self) -> list[Force]:
        forces = await self._get_response_body("forces")
        try:
            return [
                Force(id=None, name=force["name"], api_id=force["id"])
                for force in forces
            ]
        except KeyError as error:
            logging.exception("Failed to map forces returned from Police API")
            raise error

    async def get_available_dates(self) -> list[AvailableDate]:
        available_dates = await self._get_response_body(
            "crimes-street-dates", "available dates"
        )
        try:
            return [
                AvailableDate(id=None, year_month=date["date"])
                for date in available_dates
            ]
        except KeyError as error:
            logging.exception("Failed to map available dates returned from Police API")
            raise error

    async def _get_response_body(
        self, endpoint: str, type: str | None = None
    ) -> list[dict]:
        response = await self.get(endpoint)
        try:
            response.raise_for_status()
        except HTTPStatusError as error:
            message = (
                f"Failed to fetch {type} from Police API"
                if type
                else f"Failed to fetch {endpoint} from Police API"
            )
            logging.exception(message)
            raise error
        return response.json()
