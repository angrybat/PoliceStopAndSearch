from asyncio import run
from datetime import datetime

from typer import Argument, Typer

from police_api_ingester.commands.parsers import default_timezone_to_utc
from police_api_ingester.factories import (
    get_available_date_repository,
    get_force_repository,
    get_stop_and_search_repository,
)

ingest = Typer()


@ingest.command("forces")
def ingest_forces() -> None:
    force_repository = get_force_repository()
    run(force_repository.store_forces())


@ingest.command("available-dates")
def ingest_available_dates(
    from_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    to_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
) -> None:
    available_date_repository = get_available_date_repository()
    run(available_date_repository.store_available_dates(from_datetime, to_datetime))


@ingest.command("stop-and-searches")
def ingest_stop_and_searches(
    from_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    to_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    run_store_available_dates: bool = True,
) -> None:
    stop_and_search_repository = get_stop_and_search_repository()
    run(
        stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, store_available_dates=run_store_available_dates
        )
    )
