from datetime import datetime

from typer import Option

from police_api_ingester.commands.parsers import (
    CRON_EXAMPLE,
    default_timezone_to_utc,
    parse_cron,
)
from police_api_ingester.models.cron import Cron
from police_api_ingester.police_client import BASE_URL

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
LOGGING_CONF_FILE_PATH: str = Option(
    "logging.conf",
    "--logging-conf-file-path",
    help="Path to the loggig.conf file to configure the logging,",
    envvar="LOGGING_CONF_FILE_PATH",
)
DATABASE_URL: str = Option(
    ..., "--database-url", help="The Postgres Database Url.", envvar="DATABASE_URL"
)
POLICE_CLIENT_BASE_URL: str = Option(
    BASE_URL,
    "--base-url",
    help="The base url for the Police API",
    envvar="POLICE_CLIENT_BASE_URL",
)
POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS: int = Option(
    15,
    "--max-requests-per-seconds",
    help="The max number of requests per seconds that can be made to the Police API.",
    envvar="POLICE_CLIENT_MAX_REQUESTS_PER_SECONDS",
)
POLICE_CLIENT_MAX_REQUEST_RETRIES: int = Option(
    5,
    "--max-request-retries",
    help="The max retires for a request to the Police API, retires only happen when max requests are exceeded or a timeout occurs.",
    envvar="POLICE_CLIENT_MAX_REQUEST_RETRIES",
)
POLICE_CLIENT_TIMEOUT: int = Option(
    10,
    "--timeout",
    help="The max number of seconds to wait for a request to the Police API before timing out",
    envvar="POLICE_CLIENT_TIMEOUT",
)
