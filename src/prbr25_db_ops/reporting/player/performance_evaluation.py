from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres


def get_player_monthly_performance(
    sql: Postgres, id_list: list, save: bool = False, path: str = "."
) -> DataFrame:
    id_string = ", ".join(map(str, id_list))
    query = f"""SELECT p.tag, s.standing AS pos, e.tournament_name AS torneio, e.event_name AS evento, s.perf_score as pontuacao, s.player_id, p.value
                FROM standings AS s
                LEFT JOIN players AS p ON s.player_id = p.id
                LEFT JOIN raw_events AS e ON s.event_id = e.id
                WHERE s.event_id IN ({id_string}) AND s.perf_score > 0
                ORDER BY s.perf_score DESC"""
    df = sql.query_db(query, "standings")
    if save:
        df.drop(["player_id", "value"], axis=1).to_csv(
            f"{path}/performance.csv", index=False
        )
    update_player_values(sql, df)
    return df


def update_player_values(sql: Postgres, df: DataFrame) -> None:
    """
    Update player values when their pontuacao is higher than their current value.

    Args:
        sql: Postgres connection object
        df: DataFrame with columns 'player_id', 'pontuacao', and 'value'
    """
    # Filter players whose pontuacao is higher than their current value
    players_to_update = df[df["pontuacao"] > df["value"]]

    for _, row in players_to_update.iterrows():
        player_id = int(row["player_id"])
        new_value = float(row["pontuacao"])

        update_query = f"UPDATE players SET value = {new_value} WHERE id = {player_id}"
        sql.execute_update(update_query)


def notable_wins(
    sql: Postgres, id_list: list, save: bool = False, path: str = "."
) -> DataFrame:
    id_string = ", ".join(map(str, id_list))
    query = f"""SELECT
                    winner.tag AS vencedor,
                    loser.tag AS perdedor,
                    m.round AS rodada,
                    loser.value - winner.value AS dif_pts
                FROM matches AS m
                LEFT JOIN players as winner ON m.winning_player_id = winner.id
                LEFT JOIN players as loser ON m.losing_player_id = loser.id
                LEFT JOIN raw_phases as p ON p.id = m.phase_id
                WHERE p.event_id IN ({id_string})
                    AND loser.value - winner.value > 0
                    AND m.dq = False
                ORDER BY dif_pts DESC"""
    df = sql.query_db(query, "matches")
    if save:
        df.to_csv(f"{path}/upsets.csv", index=False)
    return df
