# py-udal-mgo

A [Uniform Data Access Layer (UDAL)](https://lab.fairease.eu/udal/) implementation.


## CSV table creation

### Observatories metadata
F-E QC not yet working, so currently using provisory `emo-bon-data-validataion` developed [here](https://github.com/emo-bon/emo-bon-data-validation). For observatories, I pull `validated-data/Observatory_combined_logsheets_validated.csv` [raw data link](https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv)

**Script**: `mgo/pull_observatories.py`

### Combined logsheets
The same as above, pulling batch 1 and batch 2 validated data from `validated-data/Batch1and2_combined_logsheets_2024-11-12.csv` [raw dta link](https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv)



### Combined taxonomy tables
David and Cymon assumes that the table Marc means is combined taxonomy table from more than one sampling. That is achieved using methods refactored from Cymons repo here

#### repos
https://github.com/cymon/fair-ease-mgf-data/tree/main, this one is currently private

https://github.com/cymon/fair-ease-data-transforms/tree/main

#### Alternatively
Single sampling taxonomy data example is pulled from TBC


[FAIR-EASE Lab](https://lab.fairease.eu)

