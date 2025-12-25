from pandas import DataFrame
from prbr25_logger.logger import setup_logger
from prbr25_rds_client.postgres import Postgres

logger = setup_logger(__name__)


def get_player_monthly_performance(
    sql: Postgres, id_list: list, save: bool = False, path: str = "."
) -> DataFrame:
    id_string = ", ".join(map(str, id_list))
    query = f"""SELECT p.tag, s.standing AS pos, e.tournament_name AS torneio, e.event_name AS evento, s.perf_score as pontuacao, s.player_id, p.value, e.address_state, ce.value AS event_score
                FROM standings AS s
                LEFT JOIN players AS p ON s.player_id = p.id
                LEFT JOIN raw_events AS e ON s.event_id = e.id
                LEFT JOIN consolidated_events as ce ON s.event_id = ce.id
                WHERE s.event_id IN ({id_string}) AND s.perf_score > 0
                ORDER BY s.perf_score DESC"""
    df = sql.query_db(query, "standings")
    if save:
        df.drop(["player_id", "value", "address_state", "event_score"], axis=1).to_csv(
            f"{path}/performance.csv", index=False
        )
    update_player_values(sql, df)
    return df


def update_player_values(sql: Postgres, df: DataFrame) -> None:
    """
    Update player values when their pontuacao is higher than their current value.
    Only considers rows with the highest event_score for each address_state.

    Args:
        sql: Postgres connection object
        df: DataFrame with columns 'player_id', 'pontuacao', 'value', 'address_state', and 'event_score'
    """
    # Get the max event_score for each address_state
    max_event_scores = df.groupby("address_state")["event_score"].max()

    # Filter to only keep rows with max event_score for their state
    df_filtered = df[
        df.apply(
            lambda row: row["event_score"] == max_event_scores[row["address_state"]],
            axis=1,
        )
    ]

    # Filter players whose pontuacao is higher than their current value
    players_to_update = df_filtered[df_filtered["pontuacao"] > df_filtered["value"]]

    if len(players_to_update) == 0:
        logger.info("No players increased in value")
        return

    for _, row in players_to_update.iterrows():
        player_id = int(row["player_id"])
        player_tag = row["tag"]
        old_value = float(row["value"])
        new_value = float(row["pontuacao"])

        update_query = f"UPDATE players SET value = {new_value} WHERE id = {player_id}"
        sql.execute_update(update_query)

        logger.info(f"Player {player_tag} ranked up: {old_value} -> {new_value}")


def notable_wins(
    sql: Postgres, id_list: list, save: bool = False, path: str = "."
) -> DataFrame:
    id_string = ", ".join(map(str, id_list))
    query = f"""SELECT
                    winner.tag AS vencedor,
                    loser.tag AS perdedor,
                    e.tournament_name AS torneio,
                    e.event_name AS evento,
                    m.round AS rodada,
                    loser.value - winner.value AS dif_pts
                FROM matches AS m
                LEFT JOIN players as winner ON m.winning_player_id = winner.id
                LEFT JOIN players as loser ON m.losing_player_id = loser.id
                LEFT JOIN raw_phases as p ON p.id = m.phase_id
                LEFT JOIN raw_events as e ON e.id = p.event_id
                WHERE p.event_id IN ({id_string})
                    AND loser.value - winner.value > 0
                    AND m.dq = False
                ORDER BY dif_pts DESC"""
    df = sql.query_db(query, "matches")
    if save:
        df.to_csv(f"{path}/upsets.csv", index=False)
    return df
