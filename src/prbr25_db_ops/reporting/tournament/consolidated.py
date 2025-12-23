from datetime import datetime

from dateutil.relativedelta import relativedelta
from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres
from prbr25_weights.weights import add_grade_column


def get_validated_tournaments(
    month: int, year: int, sql: Postgres, save: bool = False, path: str = ""
) -> DataFrame:
    start_date = datetime(year, month, 1)
    end_date = start_date + relativedelta(months=1)
    query = f"""SELECT r.tournament_name, r.event_name, r.address_state, r.start_at, r.num_entrants, c.value AS score, c.n_dqs, r.url
                FROM raw_events AS r
                INNER JOIN consolidated_events AS c ON r.id = c.id
                WHERE r.validated AND r.start_at BETWEEN '{start_date}' AND '{end_date}'
            """
    df = sql.query_db(query, "raw_events")
    df = add_grade_column(df)
    if save:
        df.to_csv(f"{path}/consolidated_events.csv", index=False)
    return df
