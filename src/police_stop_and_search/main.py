from typing import Annotated

from dagger import Container, Directory, Doc, dag, function, object_type

POSTGRES_TAG_DOCS = "Tag of the postgres image, defaults to 17.6-bookworm"
PYTHON_TAG_DOCS = "Tag of the python image"
USERNAME_DOCS = "Username of the superuser, defaults to postgres"
PASSWORD_DOCS = "Password of the superuser, defaults to password"
DATABASE_NAME_DOCS = "Name of the database created, defaults to postgres"
SOURCE_DOCS = "Source directory containing the codebase"


@object_type
class PoliceStopAndSearch:
    @function
    def postgres(
        self,
        tag: Annotated[str, Doc(POSTGRES_TAG_DOCS)] = "17.6-bookworm",
        username: Annotated[str, Doc(USERNAME_DOCS)] = "postgres",
        password: Annotated[str, Doc(PASSWORD_DOCS)] = "password",
        database_name: Annotated[str, Doc(DATABASE_NAME_DOCS)] = "postgres",
    ) -> Container:
        """Returns a postgres container"""
        return (
            dag.container()
            .from_(f"postgres:{tag}")
            .with_env_variable("POSTGRES_USER", username)
            .with_env_variable("POSTGRES_PASSWORD", password)
            .with_env_variable("POSTGRES_DB", database_name)
            .with_exposed_port(5432)
        )

    @function
    def postgres_service(
        self,
        tag: Annotated[str, Doc(POSTGRES_TAG_DOCS)] = "17.6-bookworm",
        username: Annotated[str, Doc(USERNAME_DOCS)] = "postgres",
        password: Annotated[str, Doc(PASSWORD_DOCS)] = "password",
        database_name: Annotated[str, Doc(DATABASE_NAME_DOCS)] = "postgres",
    ) -> Container:
        """Returns the postgres container as a service so it can be run via dagger"""
        return self.postgres(tag, username, password, database_name).as_service()

    @function
    def production_dependencies(
        self,
        source: Annotated[Directory, Doc(SOURCE_DOCS)],
        tag: Annotated[str, Doc(PYTHON_TAG_DOCS)] = "3.12-slim-bookworm",
    ) -> Container:
        """Returns a container with the production dependencies installed.
        This uses a cache volume to prevent re-installing packages on each call."""
        production_pip_cache = dag.cache_volume("production_pip_cache")
        return (
            dag.container()
            .from_(f"python:{tag}")
            .with_mounted_cache("/root/.cache/pip", production_pip_cache)
            .with_workdir("/src")
            .with_file("requirements.txt", source.file("requirements/production.txt"))
            .with_exec(["pip", "install", "-r", "requirements.txt"])
            .without_file("requirements.txt")
        )
