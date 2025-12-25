from datetime import datetime

from dateutil.relativedelta import relativedelta
from pandas import DataFrame, Series
from prbr25_rds_client.postgres import Postgres
from prbr25_weights.weights import add_grade_column


def get_validated_tournaments(
    month: int, year: int, sql: Postgres, save: bool = False, path: str = ""
) -> tuple[DataFrame, Series]:
    start_date = datetime(year, month, 1)
    end_date = start_date + relativedelta(months=1)
    query = f"""SELECT r.tournament_name AS torneio,
                    r.event_name AS evento,
                    r.address_state AS estado,
                    r.start_at AS data_de_inicio,
                    r.num_entrants AS numero_inscritos,
                    c.value AS score,
                    c.n_dqs,
                    r.url,
                    r.id
                FROM raw_events AS r
                INNER JOIN consolidated_events AS c ON r.id = c.id
                WHERE r.validated AND r.start_at BETWEEN '{start_date}' AND '{end_date}'
            """
    df = sql.query_db(query, "raw_events")
    df = add_grade_column(df)
    id_series = df["id"]
    df.drop(["id"], axis=1)
    if save:
        df.to_csv(f"{path}/consolidated_events.csv", index=False)
    return df, id_series
