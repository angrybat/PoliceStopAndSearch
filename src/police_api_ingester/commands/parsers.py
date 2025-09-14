from datetime import datetime, timezone

from police_api_ingester.factories import logger


def default_timezone_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        logger.info("Timezone not provided assuming UTC")
        return value.astimezone(timezone.utc)
    return value
