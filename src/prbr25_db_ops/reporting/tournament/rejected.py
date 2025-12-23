from datetime import datetime

from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres


def get_rejected_tournaments(
    month: int, year: int, sql: Postgres, save: bool = False, path: str = ""
) -> DataFrame:
    start_date = datetime(year, month, 1)
    end_date = start_date + relativedelta(months=1)
    query = f"""SELECT tournament_name AS nome_torneio, event_name AS nome_evento, address_state AS estado, url AS link
                FROM raw_events
                WHERE validated IS NULL AND start_at BETWEEN '{start_date}' AND '{end_date}'
            """
    df = sql.query_db(query, "raw_events")
    df["motivo"] = ""
    if save:
        df.to_csv(f"{path}/consolidated_events.csv", index=False)
    return df
