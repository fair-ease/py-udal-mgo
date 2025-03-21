import pandas as pd
import typing
from typing import Any, Literal
from pathlib import Path

import udal.specification as udal
from .namedqueries import QUERY_NAMES, QueryName
from .result import Result
from .brokers.local import LocalBroker

# SparQL endpoint will go here
Connection = Literal['']

class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, connectionString: Connection | None = None, config: udal.Config = udal.Config()):
        self._config = config
        if connectionString is None:
            self._broker = LocalBroker()
        else:
            raise Exception(f'connection string {connectionString} not supported')

    def execute(self, name: str, params: dict | None = None) -> Result:
        """Find and execute the query with the given name."""
        if name in QUERY_NAMES:
            return self._broker.execute(name, params)
        else:
            raise Exception(f'query {name} not supported')


    @property
    def queries(self) -> dict[str, udal.NamedQueryInfo]:
        return self._broker.queries
