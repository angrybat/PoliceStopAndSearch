from typing import Annotated

from dagger import Container, Directory, Doc, dag, function, object_type

POSTGRES_TAG_DOC = Doc("Tag of the postgres image")
PYTHON_TAG_DOC = Doc("Tag of the python image")
USERNAME_DOC = Doc("Username of the superuser")
PASSWORD_DOC = Doc("Password of the superuser")
DATABASE_NAME_DOC = Doc("Name of the database created")
SOURCE_DOC = Doc("Source directory containing the codebase")
CACHE_VOLUME_DOC = Doc("Name of the cache volume to use")
USER = "sgt_angle"
USER_HOME = f"/home/{USER}"
PIP_CACHE_PATH = f"{USER_HOME}/.cache/pip"


@object_type
class PoliceStopAndSearch:
    @function
    def postgres(
        self,
        tag: Annotated[str, POSTGRES_TAG_DOC] = "17.6-bookworm",
        username: Annotated[str, USERNAME_DOC] = "postgres",
        password: Annotated[str, PASSWORD_DOC] = "password",
        database_name: Annotated[str, DATABASE_NAME_DOC] = "postgres",
        cache_volume_name: Annotated[str | None, CACHE_VOLUME_DOC] = None,
    ) -> Container:
        """Returns a postgres container
        The data can be cached by providing a cache_volume_name
        """
        container = (
            dag.container()
            .from_(f"postgres:{tag}")
            .with_env_variable("POSTGRES_USER", username)
            .with_env_variable("POSTGRES_PASSWORD", password)
            .with_env_variable("POSTGRES_DB", database_name)
            .with_env_variable("PGDATA", "/var/lib/postgresql/data")
            .with_exposed_port(5432)
        )
        if cache_volume_name is not None:
            return container.with_mounted_cache(
                "/var/lib/postgresql/data", dag.cache_volume(cache_volume_name)
            )
        return container

    @function
    async def production_dependencies(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
    ) -> Container:
        """Returns a container with the production dependencies installed
        This uses a cache volume to prevent re-installing packages on each call"""
        production_pip_cache = dag.cache_volume("production_pip_cache")
        container = (
            dag.container()
            .from_(f"python:{tag}")
            .with_exec(["useradd", "-m", USER])
            .with_user(USER)
            .with_env_variable("PIP_CACHE_DIR", PIP_CACHE_PATH)
            .with_mounted_cache(PIP_CACHE_PATH, production_pip_cache)
            .with_workdir("/app")
        )
        path = await container.env_variable("PATH")
        container = container.with_env_variable(
            "PATH", f"{USER_HOME}/.local/bin:{path}"
        )
        return self.install_requirements(
            container, source, "requirements/production.txt"
        )

    @function
    async def development_dependencies(
        self,
        source: Annotated[Directory, SOURCE_DOC],
        tag: Annotated[str, PYTHON_TAG_DOC] = "3.12-slim-bookworm",
    ) -> Container:
        """Returns a container with the production and development dependencies installed
        This uses a cache volume to prevent re-installing packages on each call."""
        development_pip_cache = dag.cache_volume("development_pip_cache")
        container = await self.production_dependencies(source, tag)
        container = container.with_mounted_cache(PIP_CACHE_PATH, development_pip_cache)
        return self.install_requirements(
            container, source, "requirements/development.txt"
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
        development_container = await self.development_dependencies(source, python_tag)
        postrges_container = self.postgres(
            postgres_tag, username, password, database_name, "bronze_db_data"
        )
        postgres_service = postrges_container.as_service(use_entrypoint=True)
        await (
            development_container.with_service_binding("postgres", postgres_service)
            .with_env_variable(
                "DATABASE_URL",
                f"postgresql+psycopg2://{username}:{password}@postgres/{database_name}",
            )
            .with_directory("/app/src/alembic", source.directory("src/alembic"))
            .with_directory("/app/src/models", source.directory("src/models"))
            .with_file("/app/alembic.ini", source.file("alembic.ini"))
            .with_exec(["alembic", "upgrade", "fb1ef6ecc640"])
            .sync()
        )
        return postrges_container

    def install_requirements(
        self, container: Container, source: Directory, requirements_path: str
    ) -> Container:
        """Installs the requirements from the given path into the given container"""
        return (
            container.with_file("requirements.txt", source.file(requirements_path))
            .with_exec(["pip", "install", "--user", "-r", "requirements.txt"])
            .without_file("requirements.txt")
        )
