import typing
from typing import List, Literal

from udal.specification import NamedQueryInfo
import udal.specification as udal


QueryName = Literal[
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
"""Type to help development restricting query names to existing ones."""


QUERY_NAMES: typing.Tuple[QueryName, ...] = typing.get_args(QueryName)
"""List of the supported query names."""


# Ordered alphabetically
QUERY_REGISTRY: dict[QueryName, NamedQueryInfo] = {
    "urn:embrc.eu:emobon:all_by_ref_code": NamedQueryInfo(
        "urn:embrc.eu:emobon:all_by_ref_code",
        {
            'ref_code': ['str', udal.tlist('str')],
        },
    ),
    "urn:embrc.eu:emobon:go": NamedQueryInfo(
        "urn:embrc.eu:emobon:go",
        {
            'ref_code': ['str', udal.tlist('str')],
            'id': ['str', udal.tlist('str')],
            'name': ['str', udal.tlist('str')],
            'aspect': [
                udal.tliteral('biological_process'),
                udal.tliteral('cellular_component'),
                udal.tliteral('molecular_function'),
            ],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
        },
    ),
    "urn:embrc.eu:emobon:go_slim": NamedQueryInfo(
        "urn:embrc.eu:emobon:go_slim",
        {
            'ref_code': ['str', udal.tlist('str')],
            'id': ['str', udal.tlist('str')],
            'name': ['str', udal.tlist('str')],
            'aspect': [
                udal.tliteral('biological_process'),
                udal.tliteral('cellular_component'),
                udal.tliteral('molecular_function'),
            ],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
        },
    ),
    "urn:embrc.eu:emobon:ips": NamedQueryInfo(
        "urn:embrc.eu:emobon:ips",
        {
            'ref_code': ['str', udal.tlist('str')],
            'accession': ['str', udal.tlist('str')],
            'description': ['str', udal.tlist('str')],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
        },
    ),
    "urn:embrc.eu:emobon:ko": NamedQueryInfo(
        "urn:embrc.eu:emobon:ko",
        {
            'ref_code': ['str', udal.tlist('str')],
            'entry': ['str', udal.tlist('str')],
            'name': ['str', udal.tlist('str')],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
        },
    ),
    "urn:embrc.eu:emobon:logsheets": NamedQueryInfo(
        "urn:embrc.eu:emobon:logsheets",
        {
            'source_mat_id': ['str', udal.tlist('str')],
            'tax_id': ['int', udal.tlist('int')],
            'scientific_name': [
                udal.tliteral('marine plankton metagenome'),
                udal.tliteral('marine sediment metagenome'),
                udal.tliteral('metagenome'),
            ],
            'investigation_type': ['str', udal.tlist('str')],
            'collection_date': ['str', udal.tlist('str')],          # this should do a range
            'tidal_stage': [
                udal.tliteral('no_tide'),
                udal.tliteral('low_tide'),
                udal.tliteral('high_tide'),
                udal.tliteral('flood_tide'),
                udal.tliteral('ebb_tide'),
            ],
            'depth': ['float', udal.tlist('float')],
            'samp_size_vol': ['float', udal.tlist('float')],
            'failure': [udal.tliteral('PRESENT'), udal.tliteral('MISSING')],
            'chlorophyll': ['float', udal.tlist('float')],              # this should allow range
            'chlorophyll_method': ['PRESENT', udal.tliteral('str')],     # if value measured or not
            'sea_surf_temp': ['float', udal.tlist('float')],            # should be range
            'sea_surf_salinity': ['float', udal.tlist('float')],        # should be range
            'sea_subsurf_salinity': ['float', udal.tlist('float')],     # should be range
            'alkalinity': ['float', udal.tlist('float')],               # should be range
            'alkalinity_method': ['PRESENT', udal.tliteral('str')],      # if value measured or not
            'ammonium': ['float', udal.tlist('float')],                 # should be range
            'ammonium_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'bac_prod': ['float', udal.tlist('float')],                 # should be range
            'bac_prod_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'biomass': ['float', udal.tlist('float')],                  # should be range
            'biomass_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
            'conduc': ['float', udal.tlist('float')],                   # should be range
            'conduc_method': ['PRESENT', udal.tliteral('str')],          # if value measured or not
            'diss_carb_dioxide': ['float', udal.tlist('float')],        # should be range
            'diss_carb_dioxide_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
            'diss_inorg_carb': ['float', udal.tlist('float')],          # should be range
            'diss_inorg_carb_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
            'diss_org_carb': ['float', udal.tlist('float')],            # should be range
            'diss_org_carb_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
            'diss_org_nitro': ['float', udal.tlist('float')],            # should be range
            'diss_org_nitro_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
            'down_par': ['float', udal.tlist('float')],                 # should be range
            'down_par_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'diss_oxygen': ['float', udal.tlist('float')],              # should be range
            'diss_oxygen_method': ['PRESENT', udal.tliteral('str')],     # if value measured or not
            'n_alkanes': ['float', udal.tlist('float')],                # should be range
            'n_alkanes_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
            'nitrate': ['float', udal.tlist('float')],                  # should be range
            'nitrate_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
            'nitrite': ['float', udal.tlist('float')],                  # should be range
            'nitrite_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
            'organism_count': ['int', udal.tlist('int')],               # should be range
            'organism_count_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
            'ph': ['float', udal.tlist('float')],                       # should be range
            'ph_method': ['PRESENT', udal.tliteral('str')],              # if value measured or not
            'phaeopigments': ['float', udal.tlist('float')],            # should be range
            'phaeopigments_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
            'phosphate': ['float', udal.tlist('float')],                # should be range
            'phosphate_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
            'pigments': ['float', udal.tlist('float')],                 # should be range
            'pigments_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'pressure': ['float', udal.tlist('float')],                 # should be range
            'pressure_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'primary_prod': ['float', udal.tlist('float')],             # should be range
            'primary_prod_method': ['PRESENT', udal.tliteral('str')],    # if value measured or not
            'silicate': ['float', udal.tlist('float')],                 # should be range
            'silicate_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
            'sulfate': ['float', udal.tlist('float')],                  # should be range
            'sulfate_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
            'sulfide': ['float', udal.tlist('float')],                  # should be range
            'sulfide_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
            'turbidity': ['float', udal.tlist('float')],                # should be range
            'turbidity_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
            'water_current': ['float', udal.tlist('float')],            # should be range
            'water_current_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
            'env_package': [udal.tliteral('soft_sediment'),
                            udal.tliteral('hard_sediment'),
                            udal.tliteral('water_column')],
        },
    ),
    "urn:embrc.eu:emobon:lsu": NamedQueryInfo(
        "urn:embrc.eu:emobon:lsu",
        {
            'ref_code': ['str', udal.tlist('str')],
            'ncbi_tax_id': ['int', udal.tlist('int')],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
            'superkingdom': ['str', udal.tlist('str')],
            'kingdom': ['str', udal.tlist('str')],
            'phylum': ['str', udal.tlist('str')],
            'class': ['str', udal.tlist('str')],
            'order': ['str', udal.tlist('str')],
            'family': ['str', udal.tlist('str')],
            'genus': ['str', udal.tlist('str')],
            'species': ['str', udal.tlist('str')],
        },
    ),
    "urn:embrc.eu:emobon:observatories": NamedQueryInfo(
        "urn:embrc.eu:emobon:observatories",
        {
            'observatory_id': ['str', udal.tlist('str')],
            'country': ['str', udal.tlist('str')],
            'env_package': [udal.tliteral('soft_sediment'),
                            udal.tliteral('hard_sediment'),
                            udal.tliteral('water_column')],
            'loc_regional_mgrid': ['int', udal.tlist('int')],
        },
    ),
    "urn:embrc.eu:emobon:pfam": NamedQueryInfo(
        "urn:embrc.eu:emobon:pfam",
        {
            'ref_code': ['str', udal.tlist('str')],
            'entry': ['str', udal.tlist('str')],
            'name': ['str', udal.tlist('str')],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
        },
    ),
    "urn:embrc.eu:emobon:ssu": NamedQueryInfo(
        "urn:embrc.eu:emobon:ssu",
        {
            'ref_code': ['str', udal.tlist('str')],
            'ncbi_tax_id': ['int', udal.tlist('int')],
            'abundance': ['int', tuple(udal.tliteral('int'), udal.tliteral('int'))],
            'superkingdom': ['str', udal.tlist('str')],
            'kingdom': ['str', udal.tlist('str')],
            'phylum': ['str', udal.tlist('str')],
            'class': ['str', udal.tlist('str')],
            'order': ['str', udal.tlist('str')],
            'family': ['str', udal.tlist('str')],
            'genus': ['str', udal.tlist('str')],
            'species': ['str', udal.tlist('str')],
        },
    ),
}
"""Catalogue of query names supported by this implementation."""
