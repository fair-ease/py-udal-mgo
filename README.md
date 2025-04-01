# py-udal-mgo

A [Uniform Data Access Layer (UDAL)](https://lab.fairease.eu/udal/) implementation.

## Queries
Example queries are shown in `example.ipynb`.
TODO: add examples fro each query to the `example.ipynb`

### GO/GO slim
Accepted parameters and types for the `GO` and `GO_slim` query
- 'ref_code' (unique reference to the sequenced sample): string or list of strings
- 'id' (GO ontology id): string or list of strings
- 'name' (ontology group): string or list of strings
- 'aspect' (group parent): one of ['biological_process', 'cellular_component', 'molecular_function']
- 'abundance_lower' (filter only rows with abundance greater than): integer
- 'abundance_upper' (filter only rows with abundance less than): integer

### IPS
- 'ref_code' (unique reference to the sequenced sample): string or list of strings
- 'accession' (): string or list of strings
- 'description' (): string or list of strings
- 'abundance_lower' (filter only rows with abundance greater than): integer
- 'abundance_upper' (filter only rows with abundance less than): integer

### KO and PFAM
- 'ref_code' (unique reference to the sequenced sample): string or list of strings
- 'entry' (KEGG ontology id): string or list of strings
- 'name' (ontology group): string or list of strings
- 'abundance_lower' (filter only rows with abundance greater than): integer
- 'abundance_upper' (filter only rows with abundance less than): integer

### logsheets
- 'source_mat_id': string or list of strings
- 'tax_id': integer
- 'scientific_name': one of ['marine plankton metagenome' 'marine sediment metagenome' 'metagenome']
- 'investigation_type': string or list of strings
'collection_date': ['str', udal.tlist('str')],          # this should do a range
'tidal_stage': [
                udal.tliteral('no_tide'),
                udal.tliteral('low_tide'),
                udal.tliteral('high_tide'),
                udal.tliteral('flood_tide'),
                udal.tliteral('ebb_tide'),
            ],
- 'depth': ['float', udal.tlist('float')],
- 'samp_size_vol': ['float', udal.tlist('float')],
- 'failure': [udal.tliteral('PRESENT'), udal.tliteral('MISSING')],
- 'chlorophyll': ['float', udal.tlist('float')],              # this should allow range
- 'chlorophyll_method': ['PRESENT', udal.tliteral('str')],     # if value measured or not
- 'sea_surf_temp': ['float', udal.tlist('float')],            # should be range
- 'sea_surf_salinity': ['float', udal.tlist('float')],        # should be range
- 'sea_subsurf_salinity': ['float', udal.tlist('float')],     # should be range
- 'alkalinity': ['float', udal.tlist('float')],               # should be range
- 'alkalinity_method': ['PRESENT', udal.tliteral('str')],      # if value measured or not
- 'ammonium': ['float', udal.tlist('float')],                 # should be range
- 'ammonium_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'bac_prod': ['float', udal.tlist('float')],                 # should be range
- 'bac_prod_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'biomass': ['float', udal.tlist('float')],                  # should be range
- 'biomass_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
- 'conduc': ['float', udal.tlist('float')],                   # should be range
- 'conduc_method': ['PRESENT', udal.tliteral('str')],          # if value measured or not
- 'diss_carb_dioxide': ['float', udal.tlist('float')],        # should be range
- 'diss_carb_dioxide_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
- 'diss_inorg_carb': ['float', udal.tlist('float')],          # should be range
- 'diss_inorg_carb_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
- 'diss_org_carb': ['float', udal.tlist('float')],            # should be range
- 'diss_org_carb_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
- 'diss_org_nitro': ['float', udal.tlist('float')],            # should be range
- 'diss_org_nitro_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
- 'down_par': ['float', udal.tlist('float')],                 # should be range
- 'down_par_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'diss_oxygen': ['float', udal.tlist('float')],              # should be range
- 'diss_oxygen_method': ['PRESENT', udal.tliteral('str')],     # if value measured or not
- 'n_alkanes': ['float', udal.tlist('float')],                # should be range
- 'n_alkanes_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
- 'nitrate': ['float', udal.tlist('float')],                  # should be range
- 'nitrate_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
- 'nitrite': ['float', udal.tlist('float')],                  # should be range
- 'nitrite_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
- 'organism_count': ['int', udal.tlist('int')],               # should be range
- 'organism_count_method': ['PRESENT', udal.tliteral('str')],  # if value measured or not
- 'ph': ['float', udal.tlist('float')],                       # should be range
- 'ph_method': ['PRESENT', udal.tliteral('str')],              # if value measured or not
- 'phaeopigments': ['float', udal.tlist('float')],            # should be range
- 'phaeopigments_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
- 'phosphate': ['float', udal.tlist('float')],                # should be range
- 'phosphate_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
- 'pigments': ['float', udal.tlist('float')],                 # should be range
- 'pigments_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'pressure': ['float', udal.tlist('float')],                 # should be range
- 'pressure_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'primary_prod': ['float', udal.tlist('float')],             # should be range
- 'primary_prod_method': ['PRESENT', udal.tliteral('str')],    # if value measured or not
- 'silicate': ['float', udal.tlist('float')],                 # should be range
- 'silicate_method': ['PRESENT', udal.tliteral('str')],        # if value measured or not
- 'sulfate': ['float', udal.tlist('float')],                  # should be range
- 'sulfate_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
- 'sulfide': ['float', udal.tlist('float')],                  # should be range
- 'sulfide_method': ['PRESENT', udal.tliteral('str')],         # if value measured or not
- 'turbidity': ['float', udal.tlist('float')],                # should be range
- 'turbidity_method': ['PRESENT', udal.tliteral('str')],       # if value measured or not
- 'water_current': ['float', udal.tlist('float')],            # should be range
- 'water_current_method': ['PRESENT', udal.tliteral('str')],   # if value measured or not
- 'env_package': [udal.tliteral('soft_sediment'),
                udal.tliteral('hard_sediment'),
                udal.tliteral('water_column')],

### LSU and SSU
- 'ref_code' (unique reference to the sequenced sample): string or list of strings
- 'ncbi_tax_id': integer or list of integers
- 'abundance_lower' (filter only rows with abundance greater than): integer
- 'abundance_upper' (filter only rows with abundance less than): integer
- 'superkingdom': string or list of strings
- 'kingdom': string or list of strings
- 'phylum': string or list of strings
- 'class': string or list of strings
- 'order': string or list of strings
- 'family': string or list of strings
- 'genus': string or list of strings
- 'species': string or list of strings

### Observatories
- 'obs_id': string or list of strings
- 'country': string or list of strings
- 'env_package': one of ['soft_sediment', 'hard_sediment', 'water_column']
- 'loc_regional_mgrid': integer of list of integers

## Tables
### Observatories metadata
F-E QC not yet working, so currently using provisory `emo-bon-data-validataion` developed [here](https://github.com/emo-bon/emo-bon-data-validation). For observatories, I pull `validated-data/Observatory_combined_logsheets_validated.csv` [raw data link](https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv)

**Script**: `mgo/pull_observatories.py`


### Combined logsheets
The same as above, pulling batch 1 and batch 2 validated data from `validated-data/Batch1and2_combined_logsheets_2024-11-12.csv` [raw dta link](https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv)


### Combined taxonomy tables
Taxonomy tables consist of LSU and SSU tables, conbined from all batch 1 and batch 2 samplings and are located:
 - `contracts/metagoflow_analyses.LSU.parquet`
 - `contracts/metagoflow_analyses.SSU.parquet`


### Functional tables
Functional tables are:
 - `contracts/metagoflow_analyses.go.parquet`
 - `contracts/metagoflow_analyses.go_slim.parquet`
 - `contracts/metagoflow_analyses.ips.parquet`
 - `contracts/metagoflow_analyses.ko.parquet`
 - `contracts/metagoflow_analyses.pfam.parquet`


### NOTE
Codes which combine the tables work on local unzipped archives and are irrelevant for the final implementation of UDAL, therefore no pointers provided for that ATM. Once the workflow doing that from ro-crates exists (main responsible, Marc Portier), that can change.


#### repos
https://github.com/cymon/fair-ease-mgf-data/tree/main, this one is currently private

https://github.com/cymon/fair-ease-data-transforms/tree/main




[FAIR-EASE Lab](https://lab.fairease.eu)

