import pandas as pd
import pathlib
from typing import List

from ..broker import Broker
from ..namedqueries import NamedQueryInfo, QueryName, QUERY_NAMES, QUERY_REGISTRY
from ..result import Result



localBrokerQueryNames: List[QueryName] = [
    "urn:embrc.eu:emobon:all_by_ref_code",  # this should use ref codes from logsheets and query all the tables at once
    "urn:embrc.eu:emobon:go",               # columns 'ref_code', 'id', 'name', 'aspect', 'abundance'
    "urn:embrc.eu:emobon:go_slim",          # columns 'ref_code', 'id', 'name', 'aspect', 'abundance'
    "urn:embrc.eu:emobon:ips",              # 'ref_code', 'accession', 'description', 'abundance'
    "urn:embrc.eu:emobon:ko",               # 'ref_code', 'entry', 'name', 'abundance'
    "urn:embrc.eu:emobon:logsheets",
    # LSU: 'ref_code', 'ncbi_tax_id', 'abundance', 'superkingdom', 'kingdom','phylum', 'class', 'order', 'family', 'genus', 'species'
    "urn:embrc.eu:emobon:lsu",              
    "urn:embrc.eu:emobon:observatories",
    "urn:embrc.eu:emobon:pfam",             # 'ref_code', 'entry', 'name', 'abundance'
    # SSU: 'ref_code', 'ncbi_tax_id', 'abundance', 'superkingdom', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species'
    "urn:embrc.eu:emobon:ssu",              # SSU tables
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
    
    def __execute_all_by_ref_code(self, params: dict) -> dict:
        "This is the only query which return dictionary of DFs"
        paths = [
            ('metagoflow_analyses.go.parquet', 'go'),
            ('metagoflow_analyses.go_slim.parquet', 'go_slim'),
            ('metagoflow_analyses.ips.parquet', 'ips'),
            ('metagoflow_analyses.ko.parquet', 'ko'),
            ('metagoflow_analyses.lsu.parquet', 'lsu'),
            ('metagoflow_analyses.pfam.parquet', 'pfam'),
            ('metagoflow_analyses.ssu.parquet', 'ssu'),
            ('b12_combined_logsheets_validated.csv', 'logsheets'),
            ]

        data = {}
        for path, name in paths:
            df = pd.read_parquet(LocalBroker._datasetPath(path))
            df = self.__filter_data(df, params, 'ref_code')
            data[name] = df
        return data
    
    def __execute_go(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.go.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        data = self.__filter_data(data, params, 'id')
        data = self.__filter_data(data, params, 'name')
        data = self.__filter_data(data, params, 'aspect')
        data = self.__filter_abundance(data, params)
        return data

    def __execute_go_slim(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.go_slim.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        data = self.__filter_data(data, params, 'id')
        data = self.__filter_data(data, params, 'name')
        data = self.__filter_data(data, params, 'aspect')
        data = self.__filter_abundance(data, params)
        return data
    
    def __execute_ips(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.ips.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        data = self.__filter_data(data, params, 'accession')
        data = self.__filter_data(data, params, 'description')
        data = self.__filter_abundance(data, params)
        return data

    def __execute_ko(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.ko.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        data = self.__filter_data(data, params, 'entry')
        data = self.__filter_data(data, params, 'name')
        data = self.__filter_abundance(data, params)
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

    def __execute_lsu(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.lsu.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        # no this is int
        # data = self.__filter_data(data, params, 'ncbi_tax_id')
        if 'ncbi_tax_id' in params.keys():
            ncbi_tax_id = params['ncbi_tax_id']
            if isinstance(ncbi_tax_id, int):
                data = data.loc[data['ncbi_tax_id'] == ncbi_tax_id]
            elif isinstance(ncbi_tax_id, list):
                data = data.loc[data['ncbi_tax_id'].isin(ncbi_tax_id)]

        # abundance TODO: this has to be fixed in the parquet file, where there are floats.
        data = self.__filter_abundance(data, params, 'abundance')
        data = self.__filter_data(data, params, 'superkingdom')
        data = self.__filter_data(data, params, 'kingdom')
        data = self.__filter_data(data, params, 'phylum')
        data = self.__filter_data(data, params, 'class')
        data = self.__filter_data(data, params, 'order')
        data = self.__filter_data(data, params, 'family')
        data = self.__filter_data(data, params, 'genus')
        data = self.__filter_data(data, params, 'species')
        return data
    
    def __execute_observatories(self, params: dict):
        data = pd.read_csv(LocalBroker._datasetPath('Observatory_combined_logsheets_validated.csv'), index_col=[0])
        data = self.__filter_data(data, params, 'observatory_id')
        data = self.__filter_data(data, params, 'country')
        data = self.__filter_data(
            data,
            params,
            'env_package',
            valid_values=['soft_sediment', 'hard_sediment', 'water_column'],
            )
        # TODO: filter integers too
        # data = self.__filter_data(data, params, 'loc_regional_mgrid')

        if 'loc_regional_mgrid' in params.keys():
            loc_regional_mgrid = params['loc_regional_mgrid']
            if isinstance(loc_regional_mgrid, int):
                data = data.loc[data['loc_regional_mgrid'] == loc_regional_mgrid]
            elif isinstance(loc_regional_mgrid, list):
                data = data.loc[data['loc_regional_mgrid'].isin(loc_regional_mgrid)]

        return data
    
    def __execute_pfam(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.pfam.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        data = self.__filter_data(data, params, 'entry')
        data = self.__filter_data(data, params, 'name')
        data = self.__filter_abundance(data, params)
        return data
    
    def __execute_ssu(self, params: dict):
        data = pd.read_parquet(LocalBroker._datasetPath('metagoflow_analyses.ssu.parquet'))
        data = self.__filter_data(data, params, 'ref_code')
        # no this is int
        # data = self.__filter_data(data, params, 'ncbi_tax_id')
        if 'ncbi_tax_id' in params.keys():
            ncbi_tax_id = params['ncbi_tax_id']
            if isinstance(ncbi_tax_id, int):
                data = data.loc[data['ncbi_tax_id'] == ncbi_tax_id]
            elif isinstance(ncbi_tax_id, list):
                data = data.loc[data['ncbi_tax_id'].isin(ncbi_tax_id)]

        # abundance TODO: this has to be fixed in the parquet file, where there are floats.
        data = self.__filter_abundance(data, params, 'abundance')
        data = self.__filter_data(data, params, 'superkingdom')
        data = self.__filter_data(data, params, 'kingdom')
        data = self.__filter_data(data, params, 'phylum')
        data = self.__filter_data(data, params, 'class')
        data = self.__filter_data(data, params, 'order')
        data = self.__filter_data(data, params, 'family')
        data = self.__filter_data(data, params, 'genus')
        data = self.__filter_data(data, params, 'species')
        return data


    def execute(self, name: QueryName, params: dict | None = None) -> Result:
        query = LocalBroker._queries[name]
        queryParams = params or {}
        if name == "urn:embrc.eu:emobon:all_by_ref_code":
            return Result(query, self.__execute_all_by_ref_code(queryParams))
        elif name == "urn:embrc.eu:emobon:go":
            return Result(query, self.__execute_go(queryParams))
        elif name == "urn:embrc.eu:emobon:go_slim":
            return Result(query, self.__execute_go_slim(queryParams))
        elif name == "urn:embrc.eu:emobon:ips":
            return Result(query, self.__execute_ips(queryParams))
        elif name == "urn:embrc.eu:emobon:ko":
            return Result(query, self.__execute_ko(queryParams))
        elif name == "urn:embrc.eu:emobon:logsheets":
            return Result(query, self.__execute_logsheets(queryParams))
        elif name == "urn:embrc.eu:emobon:lsu":
            return Result(query, self.__execute_lsu(queryParams))
        elif name == "urn:embrc.eu:emobon:observatories":
            return Result(query, self.__execute_observatories(queryParams))
        elif name == "urn:embrc.eu:emobon:pfam":
            return Result(query, self.__execute_pfam(queryParams))
        elif name == "urn:embrc.eu:emobon:ssu":
            return Result(query, self.__execute_ssu(queryParams))
        else:
            if name in QUERY_NAMES:
                raise Exception(f'unsupported query name "{name}"')
            else:
                raise Exception(f'unknown query name "{name}"')
            

    ## Filtering utils ##
    def __filter_data(self, data, params, column_name, valid_values=None):
        if column_name in params.keys():
            value = params[column_name]
            if valid_values and value not in valid_values:
                raise Exception(f'invalid {column_name} "{value}"')
            
            if isinstance(value, str):
                data = data.loc[data[column_name] == value]
            elif isinstance(value, list):
                data = data.loc[data[column_name].isin(value)]
        return data

    def __filter_abundance(self, data, params):
        if 'abundance' in params.keys():
            abundance = params['abundance']
            if len(abundance) != 2:
                raise Exception(f'abundance should be a tuple of length 2')
            
            if isinstance(abundance[0], int) and isinstance(abundance[1], int):
                data = data.loc[(data['abundance'] >= abundance[0]) & (data['abundance'] <= abundance[1])]
            elif isinstance(abundance[0], int) and abundance[1] is None:
                data = data.loc[data['abundance'] >= abundance[0]]
            elif abundance[0] is None and isinstance(abundance[1], int):
                data = data.loc[data['abundance'] <= abundance[1]]
            else:
                raise Exception(f'abundance should be a tuple with at least one integer.')
        return data
