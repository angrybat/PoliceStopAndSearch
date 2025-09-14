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
    FROM_DATE,
    INGEST_AVAILABLE_DATES,
    TO_DATE,
)
from police_api_ingester.models import Cron

schedule = Typer()


def schedule_function(cron: Cron, func: Callable, args=None) -> None:
    scheduler = BlockingScheduler()
    scheduler.add_job(
        func,
        "cron",
        args=args,
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
) -> None:
    schedule_function(cron, ingest_forces)


@schedule.command("available-dates")
def schedule_ingest_available_dates(
    cron: Annotated[Cron, CRON],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
) -> None:
    schedule_function(cron, ingest_available_dates, args=(from_datetime, to_datetime))


@schedule.command("stop-and-searches")
def schedule_stop_and_searches_available_dates(
    cron: Annotated[Cron, CRON],
    from_datetime: Annotated[datetime, FROM_DATE],
    to_datetime: Annotated[datetime, TO_DATE],
    ingest_available_dates: bool = INGEST_AVAILABLE_DATES,
) -> None:
    schedule_function(
        cron,
        ingest_stop_and_searches,
        args=(from_datetime, to_datetime, ingest_available_dates),
    )
