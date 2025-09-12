from asyncio import gather
from datetime import datetime
from logging import Logger, getLogger

from httpx import HTTPStatusError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from sqlmodel import Session, and_, select

from src.ingest.police_client import PoliceClient
from src.models.bronze.available_date import AvailableDate


class StopAndSearchRepository:
    def __init__(
        self, engine: Engine, police_client: PoliceClient, logger: Logger | None = None
    ):
        self.engine = engine
        self.police_client = police_client
        self.logger = logger or getLogger("StopAndSearchRepository")

    async def store_stop_and_searches(
        self, from_datetime: datetime, to_datetime: datetime
    ) -> bool:
        with Session(self.engine) as session:
            try:
                available_dates = (
                    session.exec(
                        select(AvailableDate)
                        .where(
                            and_(
                                from_datetime.strftime("%Y-%m")
                                <= AvailableDate.year_month,
                                AvailableDate.year_month
                                <= to_datetime.strftime("%Y-%m"),
                            )
                        )
                        .options(joinedload(AvailableDate.forces))  # type: ignore
                    )
                    .unique()
                    .all()
                )
            except SQLAlchemyError:
                self.logger.exception(
                    "Cannot retreive AvailableDates from the database."
                )
                return False

        results = await gather(
            *[
                self.store_stop_and_search(
                    available_date.year_month, force.id, from_datetime, to_datetime
                )
                for available_date in available_dates
                for force in available_date.forces
            ]
        )
        return all(results)

    async def store_stop_and_search(
        self, date: str, force_id: str, from_datetime: datetime, to_datetime: datetime
    ) -> bool:
        try:
            with_location, without_location = await gather(
                self.police_client.get_stop_and_searches(
                    date, force_id, with_location=True
                ),
                self.police_client.get_stop_and_searches(
                    date, force_id, with_location=False
                ),
            )
        except HTTPStatusError:
            return False

        filtered_stop_and_searches = [
            stop_and_search
            for stop_and_search in with_location + without_location
            if from_datetime <= stop_and_search.datetime
            and stop_and_search.datetime <= to_datetime
        ]

        with Session(self.engine) as session:
            try:
                session.add_all(filtered_stop_and_searches)
                session.commit()
            except SQLAlchemyError as error:
                self.logger.warning(
                    f"Cannot store StopAndSearches in the database for '{force_id}' on date '{date}'.",
                    exc_info=error,
                )
                return False
        return True
