from mgo.udal import UDAL
from mgo.broker import Broker
from mgo.namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from mgo.result import Result
import pandas as pd
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "query_name, table_path",
    [
        ("urn:embrc.eu:emobon:go", "metagoflow_analyses.go.parquet"),
        ("urn:embrc.eu:emobon:go_slim", "metagoflow_analyses.go_slim.parquet"),
        ("urn:embrc.eu:emobon:ips", "metagoflow_analyses.ips.parquet"),
        ("urn:embrc.eu:emobon:ko", "metagoflow_analyses.ko.parquet"),
        ("urn:embrc.eu:emobon:logsheets", "b12_combined_logsheets_validated.csv"),
        ("urn:embrc.eu:emobon:lsu", "metagoflow_analyses.LSU.parquet"),
        ("urn:embrc.eu:emobon:observatories", "Observatory_combined_logsheets_validated.csv"),
        ("urn:embrc.eu:emobon:pfam", "metagoflow_analyses.pfam.parquet"),
        ("urn:embrc.eu:emobon:ssu", "metagoflow_analyses.SSU.parquet"),



    ],
)
def test_udal_tables(query_name, table_path):#table_name: str):
    """
    Test loading whole tables without parametrization.
    """
    if table_path.endswith(".csv"):
        df = pd.read_csv(
            Path(__file__).parent.parent.parent / "contracts" / table_path,
            index_col=[0],
        )
    else:
        df = pd.read_parquet(
            Path(__file__).parent.parent.parent / "contracts" / table_path,
        )
    udal = UDAL()
    result = udal.execute(query_name)
    assert isinstance(result, Result)
    assert isinstance(result.data(), pd.DataFrame)
    assert len(result.data()) == len(df)
    assert not result.data().empty
    assert set(result.data().columns) == set(df.columns)


@pytest.mark.parametrize(
    "query_name, table_path, params",
    [
        ("urn:embrc.eu:emobon:go", "metagoflow_analyses.go.parquet",
         {"abundance_lower": 5000, "abundance_upper": 10000}),
        ("urn:embrc.eu:emobon:go", "metagoflow_analyses.go.parquet",
         {"abundance_lower": 5000}),
        ("urn:embrc.eu:emobon:go", "metagoflow_analyses.go.parquet",
         {"abundance_upper": 10000}),
        ("urn:embrc.eu:emobon:go", "metagoflow_analyses.go.parquet",
         {"abundance_lower": -1000, "abundance_upper": 1000}),
    ],
)

def test_abundance_range(query_name, table_path, params):
    """
    Test loading abundance range.
    """
    df = pd.read_parquet(
        Path(__file__).parent.parent.parent / "contracts" / table_path,
    )
    # filter df
    if "abundance_lower" in params.keys():
        df = df[df["abundance"] >= params["abundance_lower"]]
    if "abundance_upper" in params.keys():
        df = df[df["abundance"] <= params["abundance_upper"]]
    # check if df is empty
    assert not df.empty
    # check if df is a dataframe
    assert isinstance(df, pd.DataFrame)

    udal = UDAL()
    result = udal.execute(query_name, params)

    # check if df has the same columns as the result
    assert set(df.columns) == set(result.data().columns)

    assert isinstance(result, Result)
    assert isinstance(result.data(), pd.DataFrame)
    assert not result.data().empty
    assert len(result.data()) == len(df)