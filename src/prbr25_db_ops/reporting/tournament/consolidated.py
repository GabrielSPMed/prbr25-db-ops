from datetime import datetime

from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres


def get_validated_tournaments(
    month: int, year: int, sql: Postgres, save: bool = False, path: str = ""
) -> DataFrame:
    start_date = datetime(year, month, 1)
    end_date = start_date + relativedelta(months=1)
    query = f"""SELECT c.tournament_name, c.event_name, c.address_state, c.start_at, c.num_entrants, r.value, r.n_dqs, c.url
                FROM raw_events AS r
                INNER JOIN consolidated_events AS c ON r.event_id = c.id
                WHERE r.start_date BETWEEN '{start_date}' AND '{end_date}'
            """
    df = sql.query_db(query, "raw_events")
    if save:
        df.to_csv(f"{path}/consolidated_events.csv", index=False)
    return df
