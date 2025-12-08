from prbr25_rds_client.postgres import Postgres


def query_event_info_from_id(sql: Postgres, id: int):
    query = f"""SELECT * FROM raw_events WHERE id = {id}"""
    return sql.query_db(query, "raw_events")
