from asyncio import gather
from collections.abc import Coroutine
from datetime import datetime
from typing import Annotated

from dagger import (
    Container,
    Directory,
    Doc,
    File,
    dag,
    function,
    object_type,
)

USER = "sgt_angle"
USER_HOME = f"/home/{USER}"
USER_LOCAL_PATH = f"{USER_HOME}/.local"
PIP_CACHE_PATH = f"{USER_HOME}/.cache/pip"
POSTGRES_BACKUPS_DIRECTORY = "/backups"
POSTGRES_BACKUPS_CACHE = "postgres_backups"
DATETIME_FORMAT = "[%Y-%m-%d|%Y-%m-%dT%H:%M:%S|%Y-%m-%d %H:%M:%S]"
DEFAULT_FROM_DATE = "2024-01-01"
DEFAULT_TO_DATE = "2024-04-01"

POSTGRES_TAG_DOC = Doc("Tag of the postgres image")
PYTHON_TAG_DOC = Doc("Tag of the python image")
USERNAME_DOC = Doc("Username of the superuser")
PASSWORD_DOC = Doc("Password of the superuser")
DATABASE_NAME_DOC = Doc("Name of the database created")
SOURCE_DOC = Doc("Source directory containing the codebase")
CACHE_VOLUME_DOC = Doc("Name of the cache volume to use")
BACKUP_FILE_DOC = Doc("A postgres backup file to restore from")
AVAILABLE_DATE_FROM_DATE_DOC = Doc(
    f"The inclusive datetime in the format '{DATETIME_FORMAT}' to ingest available dates from"
)
AVAILABLE_DATE_TO_DATE_DOC = Doc(
    f"The inclusive datetime in the format '{DATETIME_FORMAT}' to ingest available dates to"
)
STOP_AND_SEARCHES_FROM_DATE_DOC = Doc(
    f"The inclusive datetime in the format '{DATETIME_FORMAT}' to ingest stop and searches from"
)
STOP_AND_SEARCHES_TO_DATE_DOC = Doc(
    f"The inclusive datetime in the format '{DATETIME_FORMAT}' to ingest stop and searches to"
)


@object_type
class PoliceApiIngester:
    @function
    async def postgres(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
        backup_file: Annotated[File | None, BACKUP_FILE_DOC] = None,
    ) -> Container:
        """Returns a postgres container
        This can be restored from backup by providing a postgres backup file
        """
        base_container = await self.postgres_container(tag)
        container = (
            base_container.with_env_variable("POSTGRES_USER", username)
            .with_env_variable("POSTGRES_PASSWORD", password)
            .with_env_variable("POSTGRES_DB", database_name)
            .with_env_variable("PGDATA", "/var/lib/postgresql/data")
            .with_exposed_port(5432)
            .with_file(
                "/docker-entrypoint-initdb.d/restore.sh",
                source.file("postgres/restore.sh"),
            )
            .with_exec(["chmod", "+x", "/docker-entrypoint-initdb.d/restore.sh"])
        )
        if backup_file:
            return container.with_file(
                "/docker-entrypoint-initdb.d/backup.sql", backup_file
            )
        return container

    async def postgres_container(self, tag: str):
        return dag.container().from_(f"postgres:{tag}")

    @function
    async def production_dependencies(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
    ) -> Container:
        """Returns a container with the production dependencies installed
        This uses a cache volume to prevent re-installing packages on each call"""
        container = await self.create_python_container(
            source, tag, "production_pip_cache"
        )
        return self.install_requirements(container, source)

    @function
    async def development_dependencies(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
    ) -> Container:
        """Returns a container with the production and development dependencies installed
        This uses a cache volume to prevent re-installing packages on each call."""
        container = await self.create_python_container(source, tag, "dev_pip_cache")
        return self.install_requirements(container, source, ["dev"]).with_directory(
            "/app/tests", source.directory("tests")
        )

    @function
    async def unit_test(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
    ) -> str:
        """Runs the unit tests for the project and outputs the results in the terminal"""
        container = await self.development_dependencies(source, tag)
        return (
            await container.with_mounted_cache(
                "/app/.pytest_cache", dag.cache_volume("unit_pytest_cache"), owner=USER
            )
            .with_exec(["pytest", "tests/test_unit"])
            .stdout()
        )

    @function
    async def integration_test(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> str:
        """Runs the integration tests for the project and outputs the results in the terminal"""
        container, postgres_container = await gather(
            self.development_dependencies(source, python_tag),
            self.postgres(source, postgres_tag, username, password, database_name),
        )
        postgres_service = postgres_container.as_service(use_entrypoint=True)
        return (
            await container.with_service_binding("postgres", postgres_service)
            .with_mounted_cache(
                "/app/.pytest_cache",
                dag.cache_volume("integration_pytest_cache"),
                owner=USER,
            )
            .with_env_variable(
                "DATABASE_URL",
                f"postgresql+psycopg2://{username}:{password}@postgres/{database_name}",
            )
            .with_exec(["pytest", "tests/test_integration"])
            .stdout()
        )

    @function
    async def bronze_database_backup(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> File:
        """Returns postgres backup file for the bronze database tables"""
        development_container, postgres_container = await gather(
            self.development_dependencies(source, python_tag),
            self.postgres(source, postgres_tag, username, password, database_name),
        )
        postgres_service = postgres_container.as_service(use_entrypoint=True)
        await (
            development_container.with_service_binding("postgres", postgres_service)
            .with_env_variable(
                "DATABASE_URL",
                f"postgresql+psycopg2://{username}:{password}@postgres/{database_name}",
            )
            .with_directory("/app/alembic", source.directory("alembic"))
            .with_file("/app/alembic.ini", source.file("alembic.ini"))
            .with_exec(["alembic", "upgrade", "fb1ef6ecc640"])
            .sync()
        )
        return await self.backup_postgres_database(
            postgres_container,
            postgres_tag,
            username,
            password,
            database_name,
            "bronze.sql",
        )

    @function
    async def bronze_database(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> Container:
        """Returns a postgres container with the bronze database tables"""
        backup_file = await self.bronze_database_backup(
            source, python_tag, postgres_tag, username, password, database_name
        )
        return await self.postgres(
            source, postgres_tag, username, password, database_name, backup_file
        )

    @function
    async def bronze_database_with_forces_backup(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> File:
        """Returns postgres backup file for the bronze database tables with the forces ingested"""
        bronze_database = await self.ingest_data_into_bronze(
            ["forces"],
            source,
            python_tag,
            postgres_tag,
            username,
            password,
            database_name,
        )
        return await self.backup_postgres_database(
            bronze_database,
            postgres_tag,
            username,
            password,
            database_name,
            "bronze_with_forces.sql",
        )

    @function
    async def bronze_database_with_forces(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> Container:
        """Returns a postgres container with the bronze database tables and the forces ingested"""
        backup_file = await self.bronze_database_with_forces_backup(
            source, python_tag, postgres_tag, username, password, database_name
        )
        return await self.postgres(
            source, postgres_tag, username, password, database_name, backup_file
        )

    @function
    async def bronze_database_with_available_dates_backup(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        from_date: Annotated[str, AVAILABLE_DATE_FROM_DATE_DOC] = DEFAULT_FROM_DATE,
        to_date: Annotated[str, AVAILABLE_DATE_TO_DATE_DOC] = DEFAULT_TO_DATE,
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> File:
        """Returns postgres backup file for the bronze database tables with the available dates ingested"""
        bronze_database = await self.ingest_data_into_bronze(
            [
                "--from-datetime",
                from_date,
                "--to-datetime",
                to_date,
            ],
            source,
            python_tag,
            postgres_tag,
            username,
            password,
            database_name,
        )
        return await self.backup_postgres_database(
            bronze_database,
            postgres_tag,
            username,
            password,
            database_name,
            "bronze_with_available_dates.sql",
        )

    @function
    async def bronze_database_with_available_dates(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        from_date: Annotated[str, AVAILABLE_DATE_FROM_DATE_DOC] = DEFAULT_FROM_DATE,
        to_date: Annotated[str, AVAILABLE_DATE_TO_DATE_DOC] = DEFAULT_TO_DATE,
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> Container:
        """Returns a postgres container with the bronze database tables and the available dates ingested"""
        backup_file = await self.bronze_database_with_available_dates_backup(
            source,
            from_date,
            to_date,
            python_tag,
            postgres_tag,
            username,
            password,
            database_name,
        )
        return await self.postgres(
            source, postgres_tag, username, password, database_name, backup_file
        )

    @function
    async def bronze_database_with_stop_and_searchs_backup(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        from_date: Annotated[str, STOP_AND_SEARCHES_FROM_DATE_DOC] = DEFAULT_FROM_DATE,
        to_date: Annotated[str, STOP_AND_SEARCHES_TO_DATE_DOC] = DEFAULT_TO_DATE,
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> File:
        """Returns postgres backup file for the bronze database tables with the available dates ingested"""
        get_available_dates_database = self.bronze_database_with_available_dates(
            source,
            from_date,
            to_date,
            python_tag,
            postgres_tag,
            username,
            password,
            database_name,
        )
        bronze_database = await self.ingest_data_into_postgres(
            [
                "stop-and-searches",
                "--from-datetime",
                from_date,
                "--to-datetime",
                to_date,
                "--no-ingest-available-dates",
            ],
            source,
            python_tag,
            get_available_dates_database,
            username,
            password,
            database_name,
        )
        return await self.backup_postgres_database(
            bronze_database,
            postgres_tag,
            username,
            password,
            database_name,
            "bronze_with_stop_and_searchs.sql",
        )

    @function
    async def bronze_database_with_stop_and_searches(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        from_date: Annotated[str, STOP_AND_SEARCHES_FROM_DATE_DOC] = DEFAULT_FROM_DATE,
        to_date: Annotated[str, STOP_AND_SEARCHES_TO_DATE_DOC] = DEFAULT_TO_DATE,
        python_tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
        postgres_tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
    ) -> Container:
        """Returns a postgres container with the bronze database tables and the available dates ingested"""
        backup_file = await self.bronze_database_with_stop_and_searchs_backup(
            source,
            from_date,
            to_date,
            python_tag,
            postgres_tag,
            username,
            password,
            database_name,
        )
        return await self.postgres(
            source, postgres_tag, username, password, database_name, backup_file
        )

    async def create_python_container(
        self, source: Directory, tag: str, cache_volume_name: str
    ):
        pip_cache = dag.cache_volume(cache_volume_name)
        container = (
            dag.container()
            .from_(f"python:{tag}")
            .with_exec(["useradd", "-m", USER])
            .with_user(USER)
            .with_directory("/app", dag.directory(), owner=USER)
            .with_directory(USER_LOCAL_PATH, dag.directory(), owner=USER)
            .with_directory("/app/src", source.directory("src"), owner=USER)
            .with_file("/app/pyproject.toml", source.file("pyproject.toml"), owner=USER)
            .with_env_variable("PIP_CACHE_DIR", PIP_CACHE_PATH)
            .with_mounted_cache(PIP_CACHE_PATH, pip_cache, owner=USER)
            .with_workdir("/app")
        )
        path = await container.env_variable("PATH")
        return container.with_env_variable("PATH", f"{USER_HOME}/.local/bin:{path}")

    def install_requirements(
        self,
        container: Container,
        source: Directory,
        optional_dependencies: list[str] = [],
    ) -> Container:
        """Installs the dependencies from the pyproject.toml file and any optional dependencies"""
        dependencies = (
            f".[{','.join(optional_dependencies)}]" if optional_dependencies else "."
        )
        return (
            container.with_file("pyproject.toml", source.file("pyproject.toml"))
            .with_exec(["pip", "install", "--user", dependencies])
            .without_file("pyproject.toml")
        )

    async def ingest_data_into_postgres(
        self,
        ingest_command: list[str],
        source: Directory,
        python_tag: str,
        get_postgres_database: Coroutine[None, None, Container],
        username: str,
        password: str,
        database_name: str,
    ) -> Container:
        postgres_database, production_dependencies = await gather(
            get_postgres_database,
            self.production_dependencies(source, python_tag),
        )
        database_service = postgres_database.as_service(use_entrypoint=True)
        command = ["police-api-ingester", "ingest"] + ingest_command
        await (
            production_dependencies.with_service_binding("postgres", database_service)
            .with_env_variable(
                "DATABASE_URL",
                f"postgresql+psycopg2://{username}:{password}@postgres/{database_name}",
            )
            .with_exec(command)
        )

        return postgres_database

    async def ingest_data_into_bronze(
        self,
        ingest_command: list[str],
        source: Directory,
        python_tag: str,
        postgres_tag: str,
        username: str,
        password: str,
        database_name: str,
    ) -> Container:
        get_bronze_database = self.bronze_database(
            source, python_tag, postgres_tag, username, password, database_name
        )
        return await self.ingest_data_into_postgres(
            ingest_command,
            source,
            python_tag,
            get_bronze_database,
            username,
            password,
            database_name,
        )

    async def backup_postgres_database(
        self,
        postgres_container: Container,
        postgres_tag: str,
        username: str,
        password: str,
        database_name: str,
        backup_file: str,
    ) -> File:
        backup_container = await self.postgres_container(postgres_tag)
        postgres_service = postgres_container.as_service(use_entrypoint=True)
        backup_container = await (
            backup_container.with_service_binding("postgres", postgres_service)
            .with_mounted_cache(
                POSTGRES_BACKUPS_DIRECTORY,
                dag.cache_volume(POSTGRES_BACKUPS_CACHE),
                owner=username,
            )
            .with_exec(
                [
                    "pg_dump",
                    f"postgresql://{username}:{password}@postgres/{database_name}",
                    "-f",
                    f"{POSTGRES_BACKUPS_DIRECTORY}/{backup_file}",
                ]
            )
            .sync()
        )
        output_file = f"/output/{backup_file}"
        return (
            backup_container.with_directory("/output", dag.directory())
            .with_exec(["cp", f"/backups/{backup_file}", output_file])
            .file(output_file)
        )


def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, DATETIME_FORMAT)
        return True
    except ValueError:
        return False
