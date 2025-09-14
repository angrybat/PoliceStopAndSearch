from datetime import datetime

from typer import Option

from police_api_ingester.commands.parsers import (
    CRON_EXAMPLE,
    default_timezone_to_utc,
    parse_cron,
)
from police_api_ingester.models.cron import Cron

FROM_DATE: datetime = Option(
    ...,
    "--from-datetime",
    help="The inclusive date data will be ingested from.",
    callback=default_timezone_to_utc,
)
TO_DATE: datetime = Option(
    ...,
    "--to-datetime",
    help="The inclusive date data will be ingested to.",
    callback=default_timezone_to_utc,
)
INGEST_AVAILABLE_DATES: bool = Option(True, help="Ingest available dates first.")
CRON: Cron = Option(
    ...,
    "--cron-string",
    help="When to schedule the ingest task. This needs to be a valid cron string. "
    + CRON_EXAMPLE,
    parser=parse_cron,
    metavar="[minute hour day_of_month month day_of_week]",
)
