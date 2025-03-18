import pandas as pd
import pathlib
from typing import List

from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result



localBrokerQueryNames: List[QueryName] = [
    "urn:embrc.eu:emobon:observatories",
    "urn:embrc.eu:emobon:logsheets",
]


localBrokerQueries: dict[QueryName, NamedQueryInfo] = \
    { k: v for k, v in QUERY_REGISTRY.items() if k in localBrokerQueryNames }



class LocalBroker(Broker):

    _query_names: List[QueryName] = localBrokerQueryNames

    _queries: dict[QueryName, NamedQueryInfo] = localBrokerQueries

    def __init__(self):
        pass

    @property
    def queryNames(self) -> List[str]:
        return list(LocalBroker._query_names)

    @property
    def queries(self):
        return { k: v for k, v in LocalBroker._queries.items() }

    @staticmethod
    def _datasetPath(filename: str):
        base = pathlib.Path(__file__).parent.parent.parent
        return base.joinpath('contracts', filename)
    
    def __execute_observatories(self, params: dict):
        data = pd.read_csv(LocalBroker._datasetPath('Observatory_combined_logsheets_validated.csv'), index_col=[0])
        if 'observatory_id' in params.keys():
            obs_id = params['observatory_id']
            if isinstance(obs_id, str):
                data = data.loc[data['obs_id'] == obs_id]
            elif isinstance(obs_id, list):
                data = data.loc[data['obs_id'].isin(obs_id)]

        if 'country' in params.keys():
            country = params['country']
            if isinstance(country, str):
                data = data.loc[data['country'] == country]
            elif isinstance(country, list):
                data = data.loc[data['country'].isin(country)]

        if 'env_package' in params.keys():
            env_package = params['env_package']
            if env_package not in ['soft_sediment', 'hard_sediment', 'water_column']:
                raise Exception(f'invalid env_package "{env_package}"')
            
            if isinstance(env_package, str):
                data = data.loc[data['env_package'] == env_package]
            elif isinstance(env_package, list):
                data = data.loc[data['env_package'].isin(env_package)]

        if 'loc_regional_mgrid' in params.keys():
            loc_regional_mgrid = params['loc_regional_mgrid']
            if isinstance(loc_regional_mgrid, int):
                data = data.loc[data['loc_regional_mgrid'] == loc_regional_mgrid]
            elif isinstance(loc_regional_mgrid, list):
                data = data.loc[data['loc_regional_mgrid'].isin(loc_regional_mgrid)]

        return data
    
    def __execute_logsheets(self, params: dict):
        data = pd.read_csv(LocalBroker._datasetPath('b12_combined_logsheets_validated.csv'), index_col=[0])
        if 'source_mat_id' in params.keys():
            source_mat_id = params['source_mat_id']
            if isinstance(source_mat_id, str):
                data = data.loc[data['source_mat_id'] == source_mat_id]
            elif isinstance(source_mat_id, list):
                data = data.loc[data['source_mat_id'].isin(source_mat_id)]

        if 'tax_id' in params.keys():
            tax_id = params['tax_id']
            if isinstance(tax_id, int):
                data = data.loc[data['tax_id'] == tax_id]
            elif isinstance(tax_id, list):
                data = data.loc[data['tax_id'].isin(tax_id)]

        if 'scientific_name' in params.keys():
            scientific_name = params['scientific_name']
            if scientific_name not in ['marine plankton metagenome', 'marine sediment metagenome', 'metagenome']:
                raise Exception(f'invalid scientific_name "{scientific_name}"')
            
            if isinstance(scientific_name, str):
                data = data.loc[data['scientific_name'] == scientific_name]
            elif isinstance(scientific_name, list):
                data = data.loc[data['scientific_name'].isin(scientific_name)]

        if 'investigation_type' in params.keys():
            investigation_type = params['investigation_type']
            if isinstance(investigation_type, str):
                data = data.loc[data['investigation_type'] == investigation_type]
            elif isinstance(investigation_type, list):
                data = data.loc[data['investigation_type'].isin(investigation_type)]

        if 'collection_date' in params.keys():
            collection_date = params['collection_date']
            if isinstance(collection_date, str):
                data = data.loc[data['collection_date'] == collection_date]
            elif isinstance(collection_date, list):
                data = data.loc[data['collection_date'].isin(collection_date)]

        if 'tidal_stage' in params.keys():
            tidal_stage = params['tidal_stage']
            if tidal_stage not in ['no_tide', 'low_tide', 'high_tide', 'flood_tide', 'ebb_tide']:
                raise Exception(f'invalid tidal_stage "{tidal_stage}"')
            
            if isinstance(tidal_stage, str):
                data = data.loc[data['tidal_stage'] == tidal_stage]
            elif isinstance(tidal_stage, list):
                data = data.loc[data['tidal_stage'].isin(tidal_stage)]

        return data

    def execute(self, name: QueryName, params: dict | None = None) -> Result:
        query = LocalBroker._queries[name]
        queryParams = params or {}
        if name == "urn:embrc.eu:emobon:observatories":
            return Result(query, self.__execute_observatories(queryParams))
        elif name == "urn:embrc.eu:emobon:logsheets":
            return Result(query, self.__execute_logsheets(queryParams))
        else:
            if name in QUERY_NAMES:
                raise Exception(f'unsupported query name "{name}"')
            else:
                raise Exception(f'unknown query name "{name}"')
