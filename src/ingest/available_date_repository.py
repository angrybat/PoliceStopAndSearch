from asyncio import gather
from datetime import datetime

from sqlalchemy import Engine
from sqlmodel import Session

from src.ingest.force_repository import ForceRepository
from src.ingest.police_client import PoliceClient
from src.models.bronze.available_date import AvailableDate
from src.models.bronze.available_date_force_mapping import AvailableDateForceMapping
from src.models.bronze.force import Force


class AvailableDateRepository:
    def __init__(self, engine: Engine, police_client: PoliceClient):
        self.force_repository = ForceRepository(engine, police_client)
        self.police_client = police_client
        self.engine = engine

    async def store_available_dates(
        self, from_date: datetime, to_date: datetime
    ) -> None:
        forces, available_dates = await gather(
            self.police_client.get_forces(),
            self.police_client.get_available_dates(from_date, to_date),
        )
        with Session(self.engine) as session:
            session.add_all(forces)
            force_ids = [force.id for force in forces]
            for date in available_dates:
                available_date = AvailableDate(year_month=date.year_month)
                session.add(available_date)
                session.flush()
                for force_id in date.force_ids:
                    if force_id not in force_ids:
                        session.add(Force(id=force_id))
                        session.flush()
                        force_ids.append(force_id)
                    session.add(
                        AvailableDateForceMapping(
                            available_date_id=available_date.id, force_id=force_id
                        )
                    )
            session.commit()
