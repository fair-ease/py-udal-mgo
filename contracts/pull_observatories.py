#!/usr/bin/python

"""
Single use script to pull observatories from the database and write them to a file.

In case that the file exists, it shows the differences between the current and
the previous version of the file and asks the user whether to overwrite the
file with the current version.

the local save path is hardcoded to:
https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv
"""

import os
import pandas as pd
import logging

from contracts.utils_contracts import check_diffs, rewrite_file, reconfig_logger

# set root path to the parent directory
ROOT = os.path.dirname(os.path.dirname(__file__))
CSV_LOCAL_PATH = os.path.join(
    ROOT, "contracts/Observatory_combined_logsheets_validated.csv"
)  # local

logger = logging.getLogger(__name__)
reconfig_logger(level=logging.INFO)


def pull_observatories() -> pd.DataFrame:
    """Pull observatories from the database and write them to a file."""

    data = pd.read_csv(
        "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv"
    )
    return data


if __name__ == "__main__":
    logger.debug("Pulling observatories from the database...")
    data = pull_observatories()
    if check_diffs(data, path=CSV_LOCAL_PATH, logger=logger):
        rewrite_file(data, path=CSV_LOCAL_PATH)
