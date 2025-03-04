import pandas as pd
import typing
from typing import Any, Literal
from pathlib import Path

import udal.specification as udal


#
# Description of the queries
#


QueryName = Literal[
    "urn:embrc.eu:emobon:observatories",
    "urn:embrc.ue:emobon:logsheets",
]
"""Type to help development restricting query names to existing ones."""


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)
"""List of the supported query names."""


QUERY_REGISTRY: dict[QueryName, udal.NamedQueryInfo] = {
    "urn:embrc.eu:emobon:observatories": udal.NamedQueryInfo(
        "urn:embrc.eu:emobon:observatories",
        {},
    ),
    "urn:embrc.ue:emobon:logsheets": udal.NamedQueryInfo(
        "urn:embrc.ue:emobon:logsheets",
        {},
    ),
}
"""Catalogue of query names supported by this implementation."""


#
# UDAL objects
#


class Result(udal.Result):
    """Result from executing an UDAL query."""

    Type = pd.DataFrame

    def __init__(self, query: udal.NamedQueryInfo, data: Any, metadata: dict = {}):
        self._query = query
        self._data = data
        self._metadata = metadata

    @property
    def query(self):
        """Information about the query that generated the data in this
        result."""
        return self._query

    @property
    def metadata(self):
        """Metadata associated with the result data."""
        return self._metadata

    def data(self, type: type[Type] | None = None) -> Type:
        """The data of the result."""
        if type is None or type is pd.DataFrame:
            return self._data
        raise Exception(f'type "{type}" not supported')


class UDAL(udal.UDAL):
    """Uniform Data Access Layer"""

    def __init__(self, config: udal.Config = udal.Config()):
        self._config = config
        self._queryCallables = {
            "urn:embrc.eu:emobon:observatories": self.__query_observatories,
            "urn:embrc.ue:emobon:logsheets": self.__query_logsheets,
        }

    #################
    # Observatories #
    #################
    def __query_observatories(self, params: dict) -> tuple:
        path = Path(__file__).parent.parent.joinpath(
            "contracts", "Observatory_combined_logsheets_validated.csv"
        )
        data = pd.read_csv(path, index_col=[0])

        # TODO: validate user params values
        # if string, then match (type object)
        # if list of strings, then match any (object)
        # if numeric, tuple input lower and upper bounds
        # many missing values, so some should be filtered as PRESENT/MISSING

        # TODO: extend filtering the data according to params
        data = self.__filter_observatories(data, params)

        metadata = {}
        return data, metadata

    def __validate_obs_params(self, params: dict) -> None:
        raise NotImplementedError

    def __filter_observatories(self, data: pd.DataFrame, params: dict) -> pd.DataFrame:
        """This function filters the data according to the params dictionary.
        Just an example of matching single values on the columns of the data.
        """
        # iterate over the keys of the params dictionary
        for key in params:
            # if the key is not in the columns of the data, raise an exception
            if key not in data.columns:
                raise Exception(f'key "{key}" not in the columns of the data')
            # if the key is in the columns of the data, filter the data
            data = data[data[key] == params[key]]
        return data

    #############
    # Logsheets #
    #############
    def __query_logsheets(params: dict) -> tuple:
        path = Path(__file__).parent.parent.joinpath(
            "contracts", "b12_combined_logsheets_validated.csv"
        )
        data = pd.read_csv(path, index_col=[0])

        # TODO: validate

        # TODO: filter

        metadata = {}
        return data, metadata

    def __validate_logsheet_params(self, params: dict) -> None:
        raise NotImplementedError

    def __filter_logsheets(self, data: pd.DataFrame, params: dict) -> pd.DataFrame:
        """This is just an example of matching single values on the comumns of the data."""
        # iterate over the keys of the params dictionary
        for key in params:
            # if the key is not in the columns of the data, raise an exception
            if key not in data.columns:
                raise Exception(f'key "{key}" not in the columns of the data')
            # if the key is in the columns of the data, filter the data
            data = data[data[key] == params[key]]
        return data

    def execute(self, name: str, params: dict | None = None) -> Result:
        """Find and execute the query with the given name."""
        if name in self._queryCallables:
            data, metadata = self._queryCallables[name](params or {})
            return Result(QUERY_REGISTRY[name], data, metadata)
        else:
            raise Exception(f"query {name} not supported")

    @property
    def queryNames(self) -> list[str]:
        return list(QUERY_NAMES)

    @property
    def queries(self) -> dict[str, udal.NamedQueryInfo]:
        return {k: v for k, v in QUERY_REGISTRY.items()}
