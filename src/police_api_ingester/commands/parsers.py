from datetime import datetime, timezone

from croniter import croniter
from typer import BadParameter

from police_api_ingester.factories import logger
from police_api_ingester.models.cron import Cron


def parse_cron(cron_str: str) -> Cron:
    if croniter.is_valid(cron_str):
        minute, hour, day, day_of_month, day_of_week = cron_str.split()
        return Cron(
            minute=minute,
            hour=hour,
            day_of_month=day,
            month=day_of_month,
            day_of_week=day_of_week,
        )
    raise BadParameter(
        f"'{cron_str}' is an invalid cron string. "
        "The expected string should contian 5 fields (minute hour day month weekday)."
        " For example: '*/5 0 1,15 * 1-5'. "
        "This cron job runs every 5 minutes, at midnight, on the 1st and 15th of each month,"
        " but only if that day is a weekday (Monday to Friday)."
    )


def default_timezone_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        logger.info("Timezone not provided assuming UTC")
        return value.astimezone(timezone.utc)
    return value
