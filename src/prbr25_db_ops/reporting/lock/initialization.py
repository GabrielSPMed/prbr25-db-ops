from pathlib import Path

from pandas import DataFrame
from prbr25_logger.logger import setup_logger

from prbr25_db_ops.config import lock_file_name, lock_folder_name
from prbr25_db_ops.reporting.lock.verification import has_been_initialized

logger = setup_logger(__name__)


def create_monthly_tracking_dataframe(start_year: int, start_month: int) -> DataFrame:
    data = []
    year = start_year
    month = start_month

    for i in range(12):
        data.append(
            {
                "year": year,
                "month": month,
                "report": i == 0,
                "updated_values": i == 0,
            }
        )

        month += 1
        if month > 12:
            month = 1
            year += 1

    return DataFrame(data)


def init_monthly_lock(path: str, start_year: int, start_month: int):
    if not has_been_initialized(path):
        logger.info("Initializing lock logic")
        Path(f"{path}/{lock_folder_name}").mkdir(exist_ok=True)
        lock_df = create_monthly_tracking_dataframe(start_year, start_month)
        lock_df.to_csv(f"{path}/{lock_folder_name}/{lock_file_name}", index=False)
