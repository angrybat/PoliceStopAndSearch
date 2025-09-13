from asyncio import gather
from datetime import datetime
from logging import Logger, getLogger

from httpx import HTTPStatusError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from sqlmodel import Session, and_, select

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
            forces, available_dates, existing_available_dates = await gather(
                self.force_repository.store_forces(),
                self.police_client.get_available_dates(from_date, to_date),
                self.get_available_dates(from_date, to_date, with_forces=True),
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
                self.store_available_date(available_date, existing_available_dates)
                for available_date in available_dates
            ]
        )
        return all(results)

    async def store_available_date(
        self,
        available_date_with_force_ids: AvailableDateWithForceIds,
        existing_available_dates: list[AvailableDate],
    ) -> bool:
        existing_available_date = next(
            (
                date
                for date in existing_available_dates
                if date.year_month == available_date_with_force_ids.year_month
            ),
            None,
        )
        with Session(self.engine) as session:
            if existing_available_date is None:
                available_date = AvailableDate(
                    id=None, year_month=available_date_with_force_ids.year_month
                )
                try:
                    session.add(available_date)
                    session.flush()
                except SQLAlchemyError as error:
                    self.logger.warning(
                        "Cannot flush AvailableDate "
                        f"to the database for date '{available_date}'.",
                        exc_info=error,
                    )
                    return False
            else:
                available_date = existing_available_date

            try:
                mappings = [
                    AvailableDateForceMapping(
                        available_date_id=available_date.id, force_id=force_id
                    )
                    for force_id in available_date_with_force_ids.force_ids
                    if force_id not in available_date.force_ids
                ]
                session.add_all(mappings)
                session.commit()
            except SQLAlchemyError:
                self.logger.warning(
                    "Cannot store AvailableDate and AvailableDateForceMappings "
                    f"in the database for date '{available_date}'."
                )
                return False
        return True

    async def get_available_dates(
        self, from_date: datetime, to_date: datetime, with_forces: bool = False
    ) -> list[AvailableDate] | None:
        from_year_month = from_date.strftime("%Y-%m")
        to_year_month = to_date.strftime("%Y-%m")
        query = select(AvailableDate).where(
            and_(
                from_year_month <= AvailableDate.year_month,
                AvailableDate.year_month <= to_year_month,
            )
        )
        if with_forces:
            query = query.options(joinedload(AvailableDate.forces))  # type: ignore
        with Session(self.engine) as session:
            try:
                return list(session.exec(query).unique().all())
            except SQLAlchemyError:
                self.logger.exception(
                    "Could not retrieve AvailableDates from the "
                    f"database between '{from_year_month}' to '{to_year_month}'."
                )
                return None
