import os
from collections import Counter
from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import joinedload
from sqlmodel import Session, SQLModel, desc, inspect, select, text

from police_api_ingester.models import (
    AvailableDate,
    AvailableDateForceMapping,
    AvailableDateWithForceIds,
    Force,
    StopAndSearch,
)
from police_api_ingester.police_client import PoliceClient
from police_api_ingester.repositories import (
    AvailableDateRepository,
    StopAndSearchRepository,
)

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


@pytest.fixture()
def stop_and_search_repository(
    engine: Engine, police_client: PoliceClient
) -> Generator[StopAndSearchRepository, None, None]:
    setup_database(engine)
    yield StopAndSearchRepository(engine, police_client)


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
        with Session(engine) as session:
            force = Force(id="avon-and-somerset", name="Avon and Somerset Constabulary")
            session.add(force)
            available_date = AvailableDate(year_month="2023-04")
            session.add(available_date)
            session.flush()
            session.add(
                AvailableDateForceMapping(
                    available_date_id=available_date.id, force_id=force.id
                )
            )
            session.commit()

            await available_date_repository.store_available_dates(
                datetime(2023, 4, 1, tzinfo=UTC), datetime(2023, 6, 1, tzinfo=UTC)
            )

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


class TestStopAndSearchRepository:
    @pytest.mark.asyncio
    async def test_stores_stop_and_searches(
        self,
        stop_and_search_repository: StopAndSearchRepository,
        expected_stop_and_searches: list[StopAndSearch],
        engine: Engine,
    ):
        with Session(engine) as session:
            session.add_all(
                [
                    AvailableDate(
                        year_month="2024-01",
                        forces=[
                            Force(id="nottinghamshire"),
                            Force(id="leicestershire"),
                            Force(id="derbyshire"),
                        ],
                    )
                ]
            )
            session.commit()

            await stop_and_search_repository.store_stop_and_searches(
                datetime(2024, 1, 27, 12, 0, 0, tzinfo=UTC),
                datetime(2024, 2, 1, tzinfo=UTC),
            )

            stop_and_searches = session.exec(select(StopAndSearch)).all()

        assert Counter(
            [
                frozenset(stop_and_search.model_dump(exclude={"id"}))
                for stop_and_search in stop_and_searches
            ]
        ) == Counter(
            [
                frozenset(expected_stop_and_search.model_dump(exclude={"id"}))
                for expected_stop_and_search in expected_stop_and_searches
            ]
        )
