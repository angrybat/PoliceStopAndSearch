import sys
from logging import Logger, getLogger
from logging.config import fileConfig
from typing import TypeVar

from httpx import Timeout
from sqlalchemy import create_engine

from police_api_ingester.police_client import PoliceClient
from police_api_ingester.repositories.repository import Repository


def get_logger(log_file_path: str, log_level: int):
    fileConfig(log_file_path, defaults={"sys": sys})
    logger = getLogger()
    logger.setLevel(log_level)
    for handler in logger.handlers:
        handler.setLevel(log_level)
    return logger


def get_police_client(
    logger: Logger,
    police_client_base_url: str,
    police_client_max_requests_per_seconds: int,
    police_client_max_request_retries: int,
    police_client_timeout: int,
):
    return PoliceClient(
        base_url=police_client_base_url,
        timeout=Timeout(police_client_timeout),
        logger=logger,
        max_request_retries=police_client_max_request_retries,
        max_requests_per_second=police_client_max_requests_per_seconds,
    )


T = TypeVar("T", bound=Repository)


def create_repository(
    repository: type[T],
    log_level: int,
    log_file_path: str,
    database_url: str,
    police_client_base_url: str,
    police_client_max_requests_per_seconds: int,
    police_client_max_request_retries: int,
    police_client_timeout: int,
) -> T:
    logger = get_logger(log_file_path, log_level)
    engine = create_engine(database_url)
    police_client = get_police_client(
        logger,
        police_client_base_url,
        police_client_max_requests_per_seconds,
        police_client_max_request_retries,
        police_client_timeout,
    )
    return repository(engine, police_client, logger)
