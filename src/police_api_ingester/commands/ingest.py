from asyncio import run
from datetime import datetime, timezone

from typer import Argument, Typer

from police_api_ingester.factories import (
    get_available_date_repository,
    get_force_repository,
    get_stop_and_search_repository,
)

ingest = Typer()


def default_timezone_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.astimezone(timezone.utc)
    return value


@ingest.command("forces")
def store_forces_in_bronze() -> None:
    force_repository = get_force_repository()
    run(force_repository.store_forces())


@ingest.command("available-dates")
def store_available_dates(
    from_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    to_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
) -> None:
    available_date_repository = get_available_date_repository()
    run(available_date_repository.store_available_dates(from_datetime, to_datetime))


@ingest.command("stop-and-searches")
def store_stop_and_searches(
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
