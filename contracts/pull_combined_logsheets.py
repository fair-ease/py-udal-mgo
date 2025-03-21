#!/usr/bin/python

"""
Single use script to pull combined logsheets from the database and write them to a file.

* Just for batch 1 and batch 2
* In case that the file exists, it shows the differences between the current and
  the previous version of the file and asks the user whether to overwrite the
  file with the current version.

the local save path is hardcoded to:
https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/combined_logsheets_validated.csv
"""

import os
import pandas as pd
import logging

from utils_contracts import check_diffs, rewrite_file, reconfig_logger

# set root path to the parent directory
ROOT = os.path.dirname(os.path.dirname(__file__))
CSV_LOCAL_PATH = os.path.join(
    ROOT, "contracts/b12_combined_logsheets_validated.csv"
)  # local
CSV_REMOTE_PATH = "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"


logger = logging.getLogger(__name__)
reconfig_logger(level=logging.INFO)


def pull_combined_logsheets() -> pd.DataFrame:
    """Pull combined logsheets from the database and write them to a file."""

    data = pd.read_csv(CSV_REMOTE_PATH)
    return data


if __name__ == "__main__":
    logger.debug("Pulling logsheets from the database...")
    data = pull_combined_logsheets()
    if check_diffs(data, path=CSV_LOCAL_PATH, logger=logger):
        rewrite_file(data, path=CSV_LOCAL_PATH)
