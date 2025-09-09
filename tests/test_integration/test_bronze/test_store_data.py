import os
from collections.abc import Generator

import pytest
from sqlalchemy import Engine, create_engine
from sqlmodel import Session, SQLModel, inspect, select, text

from src.ingest.force_repository import ForceRepository
from src.ingest.police_client import PoliceClient
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
def force_repository(
    engine: Engine, police_client: PoliceClient
) -> Generator[ForceRepository, None, None]:
    with engine.connect() as connection:
        inspector = inspect(connection)
        schemas = inspector.get_schema_names()
        if bronze_not_exists := "bronze" not in schemas:
            connection.execute(text("CREATE SCHEMA BRONZE"))
        if table_exists := inspector.has_table(Force.__tablename__, "bronze"):
            SQLModel.metadata.drop_all(connection)
        SQLModel.metadata.create_all(connection)
        connection.commit()
        yield ForceRepository(engine, police_client)
        SQLModel.metadata.drop_all(connection)
        if table_exists:
            SQLModel.metadata.create_all(connection)
        if bronze_not_exists:
            connection.execute(text("DROP SCHEMA BRONZE"))
        connection.commit()


class TestForceRepository:
    @pytest.mark.asyncio
    async def test_store_forces(
        self,
        engine: Engine,
        force_repository: ForceRepository,
        expected_forces: list[Force],
    ) -> None:
        forces = await force_repository.store_forces()

        assert forces == expected_forces
        with Session(engine) as session:
            stored_forces = session.exec(select(Force)).all()
        assert stored_forces == expected_forces
