from asyncio import run
from datetime import datetime

from typer import Typer

from src.police_api_ingester.factories import (
    get_available_date_repository,
    get_force_repository,
    get_stop_and_search_repository,
)

ingest = Typer()


@ingest.command()
def store_forces_in_bronze() -> None:
    force_repository = get_force_repository()
    run(force_repository.store_forces())


@ingest.command()
def store_available_dates(from_datetime: datetime, to_datetime: datetime) -> None:
    available_date_repository = get_available_date_repository()
    run(available_date_repository.store_available_dates(from_datetime, to_datetime))


@ingest.command()
def store_stop_and_searches(
    from_datetime: datetime,
    to_datetime: datetime,
    run_store_available_dates: bool = True,
) -> None:
    stop_and_search_repository = get_stop_and_search_repository()
    run(
        stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, store_available_dates=run_store_available_dates
        )
    )
