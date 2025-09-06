from typing import Annotated

from dagger import Container, Doc, dag, function, object_type

TAG_DOCS = "Tag of the postgres image, defaults to 17.6-bookworm"
USERNAME_DOCS = "Username of the superuser, defaults to postgres"
PASSWORD_DOCS = "Password of the superuser, defaults to password"
DATABASE_NAME_DOCS = "Name of the database created, defaults to postgres"


@object_type
class PoliceStopAndSearch:
    @function
    def postgres(
        self,
        tag: Annotated[str, Doc(TAG_DOCS)] = "17.6-bookworm",
        username: Annotated[str, Doc(USERNAME_DOCS)] = "postgres",
        password: Annotated[str, Doc(PASSWORD_DOCS)] = "password",
        database_name: Annotated[str, Doc(DATABASE_NAME_DOCS)] = "postgres"
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
        tag: Annotated[str, Doc(TAG_DOCS)] = "17.6-bookworm",
        username: Annotated[str, Doc(USERNAME_DOCS)] = "postgres",
        password: Annotated[str, Doc(PASSWORD_DOCS)] = "password",
        database_name: Annotated[str, Doc(DATABASE_NAME_DOCS)] = "postgres"
    ) -> Container:
        """Returns the postgres container as a service so it can be run via dagger"""
        return (
            self.postgres(tag, username, password, database_name)
            .as_service()
        )
