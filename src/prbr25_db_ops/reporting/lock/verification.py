from pathlib import Path

from pandas import read_csv
from prbr25_logger.logger import setup_logger

from prbr25_db_ops.config import lock_file_name, lock_folder_name
from prbr25_db_ops.reporting.lock.utils import find_column

logger = setup_logger(__name__)


def has_been_initialized(base_path: str) -> bool:
    monthly_lock_path = Path(base_path) / lock_folder_name
    if not monthly_lock_path.exists() or not monthly_lock_path.is_dir():
        return False
    verified_months_file = monthly_lock_path / lock_file_name
    return verified_months_file.exists() and verified_months_file.is_file()


def check_lock(
    path: str,
    month: int,
    report: bool = False,
    updated_values: bool = False,
) -> bool:
    df = read_csv(f"{path}/{lock_folder_name}/{lock_file_name}")
    column = find_column(report, updated_values)
    return bool(df.loc[df["month"] == month, column].iloc[0])
