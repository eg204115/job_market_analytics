def bootstrap_schema(con):

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS raw
    """)

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS staging
    """)

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.job_postings (
            job_id VARCHAR PRIMARY KEY,
            search_query VARCHAR,
            fetched_at TIMESTAMPTZ,
            ingestion_run_id VARCHAR,
            raw_json JSON
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingest_log (
            run_id VARCHAR,
            search_query VARCHAR,
            fetched_at TIMESTAMPTZ,
            jobs_fetched INTEGER,
            jobs_inserted INTEGER,
            jobs_skipped INTEGER,
            status VARCHAR,
            error_msg VARCHAR
        )
    """)