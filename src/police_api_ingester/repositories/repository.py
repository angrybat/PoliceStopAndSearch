from logging import Logger, getLogger

from sqlalchemy import Engine

from police_api_ingester.police_client import PoliceClient


class Repository:
    def __init__(
        self, engine: Engine, police_client: PoliceClient, logger: Logger | None = None
    ):
        self.engine = engine
        self.police_client = police_client
        self.logger = logger or getLogger(self.__class__.__name__)
