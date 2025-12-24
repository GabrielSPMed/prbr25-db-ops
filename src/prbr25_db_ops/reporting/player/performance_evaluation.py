from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres


def get_player_monthly_performance(
    sql: Postgres, id_list: list, save: bool = False, path: str = "."
) -> DataFrame:
    id_string = ", ".join(map(str, id_list))
    query = f"""SELECT p.tag, e.tournament_name AS torneio, e.event_name AS evento, s.perf_score as pontuacao_performance, s.player_id
                FROM standings AS s
                LEFT JOIN players AS p ON s.player_id = p.id
                LEFT JOIN raw_events AS e ON s.event_id = e.id
                WHERE s.event_id IN ({id_string}) AND s.perf_score > 0
                ORDER BY s.perf_score DESC"""
    df = sql.query_db(query, "standings")
    if save:
        df.drop(["player_id"], axis=1).to_csv(f"{path}/performance.csv", index=False)
    return df
