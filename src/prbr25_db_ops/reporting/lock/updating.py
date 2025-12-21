from pandas import read_csv

from prbr25_db_ops.config import lock_file_name, lock_folder_name
from prbr25_db_ops.reporting.lock.utils import find_column


def update_monthly_lock_file(
    path: str, month: int, report: bool = False, updated_values: bool = False
):
    df = read_csv(f"{path}/{lock_folder_name}/{lock_file_name}")
    column = find_column(report, updated_values)
    df.loc[df["month"] == month, column] = True
    df.to_csv(f"{path}/{lock_folder_name}/{lock_file_name}", index=False)
