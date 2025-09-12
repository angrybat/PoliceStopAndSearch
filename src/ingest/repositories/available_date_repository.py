from asyncio import gather
from datetime import datetime

from sqlalchemy import Engine
from sqlmodel import Session

from src.ingest.police_client import PoliceClient
from src.ingest.repositories.force_repository import ForceRepository
from src.models.bronze.available_date import AvailableDate, AvailableDateWithForceIds
from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping
from src.models.bronze.force import Force


class AvailableDateRepository:
    def __init__(self, engine: Engine, police_client: PoliceClient):
        self.force_repository = ForceRepository(engine, police_client)
        self.police_client = police_client
        self.engine = engine

    async def store_available_dates(
        self, from_date: datetime, to_date: datetime
    ) -> bool:
        forces, available_dates = await gather(
            self.force_repository.store_forces(),
            self.police_client.get_available_dates(from_date, to_date),
        )
        if forces is None:
            return False

        missing_forces = set(
            [
                Force(id=force_id)
                for date in available_dates
                for force_id in date.force_ids
                if force_id not in forces
            ]
        )
        with Session(self.engine) as session:
            session.add_all(missing_forces)
            session.commit()

        await gather(
            *[
                self.store_available_date(available_date)
                for available_date in available_dates
            ]
        )
        return True

    async def store_available_date(
        self, available_date_with_force_ids: AvailableDateWithForceIds
    ) -> None:
        with Session(self.engine) as session:
            available_date = AvailableDate(
                year_month=available_date_with_force_ids.year_month
            )
            session.add(available_date)
            session.flush()
            for force_id in available_date_with_force_ids.force_ids:
                session.add(
                    AvailableDateForceMapping(
                        available_date_id=available_date.id, force_id=force_id
                    )
                )
            session.commit()
