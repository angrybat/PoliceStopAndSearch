from datetime import datetime
from typing import Annotated, Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from typer import Typer

from police_api_ingester.commands.ingest import (
    ingest_available_dates,
    ingest_forces,
    ingest_stop_and_searches,
)
from police_api_ingester.commands.options import (
    CRON,
    DATABASE_URL,
    FROM_DATE,
    INGEST_AVAILABLE_DATES,
    LOGGING_CONF_FILE_PATH,
    POLICE_CLIENT_BASE_URL,
    POLICE_CLIENT_MAX_REQUEST_RETRIES,
    POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    POLICE_CLIENT_TIMEOUT,
    TO_DATE,
)
from police_api_ingester.models import Cron

schedule = Typer()


def schedule_function(cron: Cron, func: Callable, **kwargs) -> None:
    scheduler = BlockingScheduler()
    scheduler.add_job(
        func,
        "cron",
        kwargs=kwargs,
        minute=cron.minute,
        hour=cron.hour,
        day=cron.day_of_month,
        month=cron.month,
        day_of_week=cron.day_of_week,
    )

    scheduler.start()


@schedule.command("forces")
def schedule_ingest_forces(
    cron: Annotated[Cron, CRON],
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    schedule_function(
        cron,
        ingest_forces,
        database_url=database_url,
        police_client_base_url=police_client_base_url,
        police_client_max_requests_per_seconds=police_client_max_requests_per_seconds,
        police_client_max_request_retries=police_client_max_request_retries,
        police_client_timeout=police_client_timeout,
        logging_conf_file_path=logging_conf_file_path,
    )


@schedule.command("available-dates")
def schedule_ingest_available_dates(
    cron: Annotated[Cron, CRON],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
) -> None:
    schedule_function(
        cron,
        ingest_available_dates,
        from_datetime=from_datetime,
        to_datetime=to_datetime,
        database_url=database_url,
        police_client_base_url=police_client_base_url,
        police_client_max_requests_per_seconds=police_client_max_requests_per_seconds,
        police_client_max_request_retries=police_client_max_request_retries,
        police_client_timeout=police_client_timeout,
        logging_conf_file_path=logging_conf_file_path,
    )


@schedule.command("stop-and-searches")
def schedule_stop_and_searches_available_dates(
    cron: Annotated[Cron, CRON],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    database_url: Annotated[str, DATABASE_URL],
    police_client_base_url: str = POLICE_CLIENT_BASE_URL,
    police_client_max_requests_per_seconds: int = POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS,
    police_client_max_request_retries: int = POLICE_CLIENT_MAX_REQUEST_RETRIES,
    police_client_timeout: int = POLICE_CLIENT_TIMEOUT,
    logging_conf_file_path: str = LOGGING_CONF_FILE_PATH,
    ingest_available_dates: bool = INGEST_AVAILABLE_DATES,
) -> None:
    schedule_function(
        cron,
        ingest_stop_and_searches,
        from_datetime=from_datetime,
        to_datetime=to_datetime,
        database_url=database_url,
        police_client_base_url=police_client_base_url,
        police_client_max_requests_per_seconds=police_client_max_requests_per_seconds,
        police_client_max_request_retries=police_client_max_request_retries,
        police_client_timeout=police_client_timeout,
        logging_conf_file_path=logging_conf_file_path,
        ingest_available_dates=ingest_available_dates,
    )
