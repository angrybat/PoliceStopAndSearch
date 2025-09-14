from asyncio import gather
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

POSTGRES_TAG_DOC = Doc("Tag of the postgres image")
PYTHON_TAG_DOC = Doc("Tag of the python image")
USERNAME_DOC = Doc("Username of the superuser")
PASSWORD_DOC = Doc("Password of the superuser")
DATABASE_NAME_DOC = Doc("Name of the database created")
SOURCE_DOC = Doc("Source directory containing the codebase")
CACHE_VOLUME_DOC = Doc("Name of the cache volume to use")
BACKUP_FILE_DOC = Doc("A postgres backup file to restore from")

USER = "sgt_angle"
USER_HOME = f"/home/{USER}"
PIP_CACHE_PATH = f"{USER_HOME}/.cache/pip"
POSTGRES_BACKUPS_DIRECTORY = "/backups"
POSTGRES_BACKUP_FILE = f"{POSTGRES_BACKUPS_DIRECTORY}/backup.sql"


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
        container = await self.create_python_container(
            source, tag, "development_pip_cache"
        )
        return self.install_requirements(container, source, ["dev"])

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
        """Returns postgres backup file with for the bronze database tables"""
        development_container, postgres_container, backup_container = await gather(
            self.development_dependencies(source, python_tag),
            self.postgres(source, postgres_tag, username, password, database_name),
            self.postgres_container(postgres_tag),
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
        backup_container = await (
            backup_container.with_service_binding("postgres", postgres_service)
            .with_exec(
                [
                    "pg_dump",
                    f"postgresql://{username}:{password}@postgres/{database_name}",
                    "-f",
                    "backup.sql",
                ]
            )
            .sync()
        )
        return backup_container.file("backup.sql")

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
        backup_file = await self.bronze_database_backup(
            source, python_tag, postgres_tag, username, password, database_name
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
            .with_mounted_cache(PIP_CACHE_PATH, pip_cache, owner=USER)
            .with_user(USER)
            .with_env_variable("PIP_CACHE_DIR", PIP_CACHE_PATH)
            .with_directory("/app/src", source.directory("src"), owner=USER)
            .with_file("/app/pyproject.toml", source.file("pyproject.toml"), owner=USER)
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
            .with_exec(["pip", "install", dependencies])
            .without_file("pyproject.toml")
        )
