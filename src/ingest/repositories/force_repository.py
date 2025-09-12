from logging import Logger, getLogger

from httpx import HTTPStatusError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from src.ingest.police_client import PoliceClient
from src.models.bronze.force import Force


class ForceRepository:
    def __init__(
        self, engine: Engine, police_client: PoliceClient, logger: Logger | None = None
    ):
        self.engine = engine
        self.police_client = police_client
        self.logger = logger or getLogger("ForceRepository")

    async def store_forces(self) -> list[Force] | None:
        try:
            forces = await self.police_client.get_forces()
        except HTTPStatusError:
            return None

        with Session(self.engine) as session:
            try:
                session.add_all(forces)
                session.commit()
            except SQLAlchemyError:
                self.logger.exception("Could not store Forces in the database.")
                return None

            try:
                for force in forces:
                    session.refresh(force)
            except SQLAlchemyError:
                self.logger.exception("Could not refresh Forces in the database.")
                return None
        return forces
