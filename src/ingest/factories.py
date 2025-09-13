import logging
import os
import sys
from logging import Logger, basicConfig

from sqlalchemy import create_engine

from src.ingest.police_client import BASE_URL, PoliceClient
from src.ingest.repositories.available_date_repository import AvailableDateRepository
from src.ingest.repositories.force_repository import ForceRepository
from src.ingest.repositories.stop_and_search_repository import StopAndSearchRepository

basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger()


def get_engine(logger: Logger):
    database_url = os.getenv("DATABASE_URL")
    if database_url is None:
        error_message = "Cannot create database engine as the 'DATABASE_URL' enviroment variable is not set"
        logger.critical(error_message)
        raise RuntimeError(error_message)
    engine = create_engine(database_url)
    return engine


engine = get_engine(logger)


def get_police_client(logger: Logger):
    base_url = os.getenv("POLICE_API_BASE_URL", BASE_URL)
    max_requests_per_second = int(os.getenv("POLICE_API_MAX_REQUESTS_PER_SECOND", 15))
    max_request_retries = int(os.getenv("POLICE_API_MAX_REQUEST_RETRIES", 5))
    police_client = PoliceClient(
        base_url=base_url,
        logger=logger,
        max_request_retries=max_request_retries,
        max_requests_per_second=max_requests_per_second,
    )

    return police_client


police_client = get_police_client(logger)


def get_force_repository():
    return ForceRepository(engine, police_client, logger)


def get_available_date_repository():
    return AvailableDateRepository(engine, police_client, logger)


def get_stop_and_search_repository():
    return StopAndSearchRepository(engine, police_client, logger)
