from httpx import AsyncClient

from src.models.bronze.force import Force

BASE_URL = "https://data.police.uk/api/"


class PoliceClient(AsyncClient):
    def __init__(self, base_url: str = BASE_URL):
        super().__init__(base_url=base_url)

    async def get_forces(self) -> list[Force]:
        response = await self.get("forces")
        forces = response.json()
        return [
            Force(id=None, name=force["name"], api_id=force["id"]) for force in forces
        ]
