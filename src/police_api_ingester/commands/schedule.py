from datetime import datetime
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from typer import Argument, Typer

from police_api_ingester.commands.ingest import (
    ingest_available_dates,
    ingest_forces,
    ingest_stop_and_searches,
)
from police_api_ingester.commands.parsers import default_timezone_to_utc, parse_cron
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
def schedule_ingest_forces(cron: Cron = Argument(..., click_type=parse_cron)) -> None:
    schedule_function(cron, ingest_forces)


@schedule.command("available-dates")
def schedule_ingest_available_dates(
    cron: Cron = Argument(..., click_type=parse_cron),
    from_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    to_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
) -> None:
    schedule_function(cron, ingest_available_dates, args=(from_datetime, to_datetime))


@schedule.command("stop-and-searches")
def schedule_stop_and_searches_available_dates(
    cron: Cron = Argument(..., click_type=parse_cron),
    from_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    to_datetime: datetime = Argument(..., callback=default_timezone_to_utc),
    run_store_available_dates: bool = True,
) -> None:
    schedule_function(
        cron,
        ingest_stop_and_searches,
        args=(from_datetime, to_datetime, run_store_available_dates),
    )
