from asyncio import run
from datetime import datetime
from typing import Annotated

from typer import Typer

from police_api_ingester.commands.options import (
    DATABASE_URL,
    FROM_DATE,
    LOGGING_CONF_FILE_PATH,
    POLICE_CLIENT_BASE_URL,
    POLICE_CLIENT_MAX_REQUEST_RETRIES,
    POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    POLICE_CLIENT_TIMEOUT,
    TO_DATE,
)
from police_api_ingester.factories import (
    create_repository,
)
from police_api_ingester.repositories.available_date_repository import (
    AvailableDateRepository,
)
from police_api_ingester.repositories.force_repository import ForceRepository
from police_api_ingester.repositories.stop_and_search_repository import (
    StopAndSearchRepository,
)

ingest = Typer()


@ingest.command("forces")
def ingest_forces(
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    force_repository = create_repository(
        ForceRepository,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    run(force_repository.store_forces())


@ingest.command("available-dates")
def ingest_available_dates(
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    available_date_repository = create_repository(
        AvailableDateRepository,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    run(available_date_repository.store_available_dates(from_datetime, to_datetime))


@ingest.command("stop-and-searches")
def ingest_stop_and_searches(
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    stop_and_search_repository = create_repository(
        StopAndSearchRepository,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    run(
        stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, store_available_dates=ingest_available_dates
        )
    )
