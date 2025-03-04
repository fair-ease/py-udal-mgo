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

from utils_contracts import check_diffs, rewrite_file, reconfig_logger

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


# def check_diffs(data: pd.DataFrame) -> bool:
#     """Check differences between the current and the previous version of the file."""

#     # if the file does not exist, there is no need to show the differences
#     if not os.path.exists(CSV_LOCAL_PATH):
#         logger.info("Observatories file does not exist, saving pulled one.")
#         data.to_csv(CSV_LOCAL_PATH)
#         return False

#     previous = pd.read_csv(CSV_LOCAL_PATH, index_col=[0])

#     diffs = data.compare(previous, result_names=("current", "previous"))
#     if diffs.empty:
#         logger.info("No differences between the current and the previous version of the file.")
#         return False

#     logger.info("Differences between the current and the previous version of the file:")
#     logger.info(diffs)
#     return True


# def rewrite_file(data: pd.DataFrame, path: str) -> None:
#     """Ask for confirmation and rewrite the file."""

#     if input("Do you want to overwrite the file with the current version? [Y/n] ").lower() == "y":
#         data.to_csv(path)


if __name__ == "__main__":
    logger.debug("Pulling observatories from the database...")
    data = pull_observatories()
    if check_diffs(data, path=CSV_LOCAL_PATH, logger=logger):
        rewrite_file(data, path=CSV_LOCAL_PATH)
