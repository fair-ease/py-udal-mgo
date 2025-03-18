import os
import pandas as pd
import logging


FORMAT = "%(levelname)s | %(name)s | %(message)s"  # for logger


def check_diffs(data: pd.DataFrame, path: str, logger) -> bool:
    """Check differences between the current and the previous version of the file."""

    # if the file does not exist, there is no need to show the differences
    if not os.path.exists(path):
        logger.info("Observatories file does not exist, saving pulled one.")
        data.to_csv(path)
        return False

    previous = pd.read_csv(path, index_col=[0])

    diffs = data.compare(previous, result_names=("current", "previous"))
    if diffs.empty:
        logger.info(
            "No differences between the current and the previous version of the file."
        )
        return False

    logger.info("Differences between the current and the previous version of the file:")
    logger.info(diffs)
    return True


def rewrite_file(data: pd.DataFrame, path: str) -> None:
    """Ask for confirmation and rewrite the file."""

    if (
        input(
            "Do you want to overwrite the file with the current version? [Y/n] "
        ).lower()
        == "y"
    ):
        data.to_csv(path)


def reconfig_logger(format=FORMAT, level=logging.INFO):
    """(Re-)configure logging"""
    logging.basicConfig(format=format, level=level, force=True)
    logging.debug("Logging.basicConfig completed successfully")
