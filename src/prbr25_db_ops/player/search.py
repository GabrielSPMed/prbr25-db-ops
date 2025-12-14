from pandas import DataFrame
from prbr25_rds_client.postgres import Postgres


def is_player_consolidated(sql: Postgres, id: int) -> bool:
    """
    Search for a player by ID in the players table.

    Args:
        sql: Postgres connection object
        id: Player ID to search for

    Returns:
        True if player exists, False otherwise
    """
    table_name = "players"
    query = f"SELECT 1 FROM {table_name} WHERE id = {id} LIMIT 1"
    result = sql.query_db(query, table_name)

    return len(result) > 0


def fetch_all_players(sql: Postgres) -> DataFrame:
    table_name = "players"
    query = f"SELECT * FROM {table_name}"
    return sql.query_db(query, table_name)


def get_tag_from_player_id(sql: Postgres, id: int) -> str:
    table_name = "players"
    query = f"SELECT tag FROM {table_name} WHERE id = {id}"
    result = sql.query_db(query, table_name)
    return result["tag"].iloc[0]
