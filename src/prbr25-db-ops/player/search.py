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
