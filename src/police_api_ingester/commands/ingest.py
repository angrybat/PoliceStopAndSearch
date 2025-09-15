from asyncio import run
from datetime import datetime
from typing import Annotated

from typer import Typer

from police_api_ingester.commands.options import (
    DATABASE_URL,
    FORCE_IDS,
    FROM_DATE,
    INGEST_AVAILABLE_DATES,
    LOG_LEVEL,
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


@ingest.command(
    "forces",
    help="Ingests the Police Forces into the bronze database using the Police API",
)
def ingest_forces(
    database_url: Annotated[str, DATABASE_URL],
    force_ids: str | None = FORCE_IDS,
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    log_level: int = LOG_LEVEL,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    force_repository = create_repository(
        ForceRepository,
        log_level,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    force_ids_list = force_ids.split(",") if force_ids else None
    run(force_repository.store_forces(force_ids_list))


@ingest.command(
    "available-dates",
    help="Ingests the dates that have available stop and searches into the bronze database using the Police API. This will ingest the forces first.",
)
def ingest_available_dates(
    database_url: Annotated[str, DATABASE_URL],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    force_ids: str | None = FORCE_IDS,
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    log_level: int = LOG_LEVEL,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    available_date_repository = create_repository(
        AvailableDateRepository,
        log_level,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    force_ids_list = force_ids.split(",") if force_ids else None
    run(
        available_date_repository.store_available_dates(
            from_datetime, to_datetime, force_ids_list
        )
    )


@ingest.command(
    "stop-and-searches",
    help="Ingests the stop and searches into the bronze database using the Police API. By default this will ingest the available dates and forces into the database.",
)
def ingest_stop_and_searches(
    database_url: Annotated[str, DATABASE_URL],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    force_ids: str | None = FORCE_IDS,
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    ingest_available_dates: bool = INGEST_AVAILABLE_DATES,
    log_level: int = LOG_LEVEL,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    stop_and_search_repository = create_repository(
        StopAndSearchRepository,
        log_level,
        logging_conf_file_path,
        database_url,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    force_ids_list = force_ids.split(",") if force_ids is not None else None
    run(
        stop_and_search_repository.store_stop_and_searches(
            from_datetime,
            to_datetime,
            store_available_dates=ingest_available_dates,
            force_ids=force_ids_list,
        )
    )
