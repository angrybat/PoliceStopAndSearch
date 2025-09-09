from sqlalchemy import Engine
from sqlmodel import Session

from src.ingest.police_client import PoliceClient
from src.models.bronze.force import Force


class ForceRepository:
    def __init__(self, engine: Engine, police_client: PoliceClient):
        self.engine = engine
        self.police_client = police_client

    async def store_forces(self) -> list[Force]:
        forces = await self.police_client.get_forces()
        with Session(self.engine) as session:
            session.add_all(forces)
            session.commit()
            for force in forces:
                session.refresh(force)
        return forces
