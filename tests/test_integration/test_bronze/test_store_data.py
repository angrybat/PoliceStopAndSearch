import os
from collections.abc import Generator
from datetime import datetime

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import joinedload
from sqlmodel import Session, SQLModel, desc, inspect, select, text

from src.ingest.available_date_repository import AvailableDateRepository
from src.ingest.police_client import PoliceClient
from src.models.bronze.available_date import AvailableDate, AvailableDateWithForceIds
from src.models.bronze.force import Force

DEFAULT_DB_URL = "postgresql+psycopg2://postgres:password@localhost:5432/postgres"


@pytest.fixture(scope="session")
def database_connection_string() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DB_URL)


@pytest.fixture(scope="session")
def engine(database_connection_string: str) -> Engine:
    return create_engine(
        database_connection_string, connect_args={"connect_timeout": 5}
    )


@pytest.fixture()
def police_client() -> PoliceClient:
    return PoliceClient()


@pytest.fixture()
def available_date_repository(
    engine: Engine, police_client: PoliceClient
) -> Generator[AvailableDateRepository, None, None]:
    setup_database(engine)
    yield AvailableDateRepository(engine, police_client)


def setup_database(engine):
    with engine.connect() as connection:
        inspector = inspect(connection)
        schemas = inspector.get_schema_names()
        SQLModel.metadata.drop_all(connection)
        if "bronze" not in schemas:
            connection.execute(text("CREATE SCHEMA BRONZE"))
        SQLModel.metadata.create_all(connection)
        connection.commit()


class TestAvailableDateRepository:
    @pytest.mark.asyncio
    async def test_store_available_dates(
        self,
        engine: Engine,
        available_date_repository: AvailableDateRepository,
        expected_available_dates: list[AvailableDateWithForceIds],
        expected_forces: list[Force],
    ) -> None:
        await available_date_repository.store_available_dates(
            datetime(2023, 4, 1), datetime(2023, 6, 1)
        )

        with Session(engine) as session:
            stored_forces = session.exec(select(Force)).all()
            stored_available_dates = (
                session.exec(
                    select(AvailableDate)
                    .order_by(desc(AvailableDate.year_month))
                    .options(joinedload(AvailableDate.forces))  # type: ignore
                )
                .unique()
                .all()
            )
        assert sorted(stored_forces, key=lambda force: force.id) == sorted(
            expected_forces, key=lambda force: force.id
        )
        assert stored_available_dates == expected_available_dates
