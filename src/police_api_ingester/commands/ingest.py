from asyncio import run
from datetime import datetime
from typing import Annotated

from typer import Typer

from police_api_ingester.commands.options import (
    FROM_DATE,
    INGEST_AVAILABLE_DATES,
    TO_DATE,
)
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
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
) -> None:
    available_date_repository = get_available_date_repository()
    run(available_date_repository.store_available_dates(from_datetime, to_datetime))


@ingest.command("stop-and-searches")
def ingest_stop_and_searches(
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    ingest_available_dates: bool = INGEST_AVAILABLE_DATES,
) -> None:
    stop_and_search_repository = get_stop_and_search_repository()
    run(
        stop_and_search_repository.store_stop_and_searches(
            from_datetime, to_datetime, store_available_dates=ingest_available_dates
        )
    )
