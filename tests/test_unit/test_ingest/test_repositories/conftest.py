from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Engine
from sqlmodel import Session

from police_api_ingester.police_client import PoliceClient


@pytest.fixture
def mock_police_client() -> PoliceClient:
    return Mock(spec=PoliceClient)


@pytest.fixture
def mock_engine() -> Engine:
    return Mock(spec=Engine)


@pytest.fixture
def mock_session() -> Generator[Session, None, None]:
    with patch.object(
        Session, "__enter__", new_callable=Mock(spec=Session)
    ) as mock_session_enter:
        mock_session = Mock()
        mock_session_enter.return_value = mock_session
        yield mock_session
