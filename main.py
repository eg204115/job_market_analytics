from database import get_connection
from schemas import bootstrap_schema
from logging_config import setup_logging
from ingestion.ingest_jobs import ingest_query

SEARCH_QUERIES = [
    "data engineer",
    "data scientist",
    "machine learning engineer"
]


def main():

    logger = setup_logging()

    con = get_connection()

    try:

        bootstrap_schema(con)

        for query in SEARCH_QUERIES:

            stats = ingest_query(
                con,
                query,
                logger
            )

            logger.info(
                "%s -> %s",
                query,
                stats
            )

    finally:
        con.close()


if __name__ == "__main__":
    main()