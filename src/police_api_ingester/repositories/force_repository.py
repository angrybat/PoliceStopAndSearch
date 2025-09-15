from asyncio import gather
from logging import Logger

from httpx import HTTPStatusError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select

from police_api_ingester.models import Force
from police_api_ingester.police_client import PoliceClient
from police_api_ingester.repositories.repository import Repository


class ForceRepository(Repository):
    def __init__(
        self, engine: Engine, police_client: PoliceClient, logger: Logger | None = None
    ):
        super().__init__(engine, police_client, logger)

    async def store_forces(
        self, force_ids: list[str] | None = None
    ) -> list[Force] | None:
        try:
            forces, existing_forces = await gather(
                self.police_client.get_forces(force_ids), self.get_all_forces()
            )
        except HTTPStatusError:
            return None

        if existing_forces is None:
            return None

        with Session(self.engine) as session:
            forces_to_add = [force for force in forces if force not in existing_forces]
            try:
                session.add_all(forces_to_add)
                session.commit()
            except SQLAlchemyError:
                self.logger.exception("Could not store Forces in the database.")
                return None

            try:
                for force in forces_to_add:
                    session.refresh(force)
            except SQLAlchemyError:
                self.logger.exception("Could not refresh Forces in the database.")
                return None
        return existing_forces + forces_to_add

    async def get_all_forces(self) -> list[Force] | None:
        try:
            with Session(self.engine) as session:
                return list(session.exec(select(Force)).all())
        except SQLAlchemyError:
            self.logger.exception("Cannot get existing Forces from the database.")
            return None
