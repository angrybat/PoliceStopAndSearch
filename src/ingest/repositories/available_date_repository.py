from asyncio import gather
from datetime import datetime
from logging import Logger, getLogger

from httpx import HTTPStatusError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session

from src.ingest.police_client import PoliceClient
from src.ingest.repositories.force_repository import ForceRepository
from src.models.bronze.available_date import AvailableDate, AvailableDateWithForceIds
from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping
from src.models.bronze.force import Force


class AvailableDateRepository:
    def __init__(
        self, engine: Engine, police_client: PoliceClient, logger: Logger | None = None
    ):
        self.force_repository = ForceRepository(engine, police_client)
        self.police_client = police_client
        self.engine = engine
        self.logger = logger or getLogger("AvailableDateRepository")

    async def store_available_dates(
        self, from_date: datetime, to_date: datetime
    ) -> bool:
        try:
            forces, available_dates = await gather(
                self.force_repository.store_forces(),
                self.police_client.get_available_dates(from_date, to_date),
            )
        except HTTPStatusError:
            return False

        if forces is None:
            return False

        missing_forces = set(
            [
                Force(id=force_id)
                for available_date in available_dates
                for force_id in available_date.force_ids
                if force_id not in forces
            ]
        )
        if missing_forces:
            with Session(self.engine) as session:
                try:
                    session.add_all(missing_forces)
                    session.commit()
                except SQLAlchemyError:
                    self.logger.exception(
                        "Cannot store missing Forces in the database."
                    )
                    return False

        results = await gather(
            *[
                self.store_available_date(available_date)
                for available_date in available_dates
            ]
        )
        return all(results)

    async def store_available_date(
        self, available_date_with_force_ids: AvailableDateWithForceIds
    ) -> bool:
        with Session(self.engine) as session:
            available_date = AvailableDate(
                id=None, year_month=available_date_with_force_ids.year_month
            )
            try:
                session.add(available_date)
                session.flush()
            except SQLAlchemyError as error:
                self.logger.warning(
                    f"Cannot flush AvailableDate to the database for date '{available_date}'.",
                    exc_info=error,
                )
                return False

            try:
                for force_id in available_date_with_force_ids.force_ids:
                    session.add(
                        AvailableDateForceMapping(
                            available_date_id=available_date.id, force_id=force_id
                        )
                    )
                session.commit()
            except SQLAlchemyError:
                self.logger.warning(
                    f"Cannot store AvailableDate and AvailableDateForceMappings in the database for date '{available_date}'."
                )
                return False
        return True
