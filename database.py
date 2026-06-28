import duckdb
from config import config


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(config.db_path)