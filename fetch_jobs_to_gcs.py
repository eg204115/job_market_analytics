"""
Job Analytics ELT Pipeline — Step 1: Ingestion
================================================
Best practices applied:
  ✓ ELT pattern     — raw JSON lands untouched; transform later
  ✓ Idempotency     — safe to re-run; dedup on job_id
  ✓ Landing zone    — raw.job_postings preserves full API fidelity
  ✓ Audit logging   — every run writes a log row
  ✓ Secrets in env  — no hardcoded API keys
  ✓ UTC timestamps  — always store timezone-aware datetimes
  ✓ Schema layers   — raw / staging / mart namespacing from day one
  ✓ Quota-aware     — date_posted=week avoids redundant fetches

Free stack (zero cost):
  pip install duckdb requests python-dotenv

.env file (never commit this):
  RAPID_KEY=your_rapidapi_key_here
  RAPID_HOST=jsearch.p.rapidapi.com
"""

import os
import json
import time
import hashlib
import logging
import requests
import duckdb
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────

load_dotenv()  # reads .env file — secrets never hardcoded in source

RAPID_KEY  = os.getenv("RAPID_KEY")
RAPID_HOST = os.getenv("RAPID_HOST", "jsearch.p.rapidapi.com")
DB_PATH    = "jobs_warehouse.duckdb"  # single file = your local warehouse

# Customise these search queries to fit your analysis goals
SEARCH_QUERIES = [
    "data engineer",
    "analytics engineer",
    "data analyst",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Warehouse layer ─────────────────────────────────────────────────────────────
# Best practice: name schemas after pipeline layers (raw → staging → mart)
# This mirrors Snowflake/BigQuery project conventions used professionally.

def get_connection() -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB warehouse file. Creates it if it doesn't exist."""
    return duckdb.connect(DB_PATH)


def bootstrap_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the raw layer tables if they don't exist.

    raw.job_postings  — landing zone: one row per job, full JSON blob
    raw.ingest_log    — audit log: one row per pipeline run per query

    Why store raw JSON instead of parsed columns?
    ELT pattern: you transform AFTER loading. This means:
      1. If you discover a new field (e.g. salary), it's already in raw_json
      2. You can evolve your dbt models without re-calling the API
      3. Full fidelity — nothing is lost at ingest time
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # PRIMARY KEY on job_id enforces dedup at the database level (safety net)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.job_postings (
            job_id          VARCHAR PRIMARY KEY,
            search_query    VARCHAR,
            fetched_at      TIMESTAMPTZ,
            raw_json        JSON
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.ingest_log (
            run_id          VARCHAR,
            search_query    VARCHAR,
            fetched_at      TIMESTAMPTZ,
            jobs_fetched    INTEGER,
            jobs_inserted   INTEGER,
            jobs_skipped    INTEGER,
            status          VARCHAR,
            error_msg       VARCHAR
        )
    """)

    # Create staging and mart schemas now so dbt can target them in Step 2
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS mart")

    log.info("Schema bootstrapped: raw / staging / mart")


# ── API client ──────────────────────────────────────────────────────────────────

def search_jobs(query: str) -> list[dict]:
    """
    Call JSearch /search endpoint.

    Best practices:
    - timeout=15: never let a slow API hang your pipeline
    - raise_for_status(): fail loud on 4xx/5xx — silent failures are worse
    - date_posted=week: fetch only fresh listings, preserves monthly quota
    """
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "x-rapidapi-key":  RAPID_KEY,
        "x-rapidapi-host": RAPID_HOST,
    }
    params = {
        "query":       query,
        "page":        "1",
        "num_pages":   "1",
        "date_posted": "week",  # quota saver: only last 7 days
    }

    log.info(f"  GET /search  query={query!r}")
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    jobs = resp.json().get("data", [])
    log.info(f"  -> {len(jobs)} jobs returned from API")
    return jobs


def get_job_details(job_id: str) -> dict | None:
    """
    Enrich a single job with /job-details (optional, use sparingly).
    Each call costs 1 request from your 200/month free quota.
    """
    url = f"https://jsearch.p.rapidapi.com/job-details?job_id={job_id}"
    headers = {
        "x-rapidapi-key":  RAPID_KEY,
        "x-rapidapi-host": RAPID_HOST,
    }
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 200:
        return resp.json().get("data", [None])[0]
    log.warning(f"  job-details failed: {job_id} -> {resp.status_code}")
    return None


# ── Ingestion logic ─────────────────────────────────────────────────────────────

def ingest_query(con: duckdb.DuckDBPyConnection, query: str) -> dict:
    """
    Pull jobs for one search query and upsert into raw.job_postings.

    Idempotency: checking job_id before insert means this function is safe
    to run multiple times. Re-running produces the same warehouse state —
    a fundamental property of reliable data pipelines.
    """
    run_id     = hashlib.md5(f"{query}{datetime.now()}".encode()).hexdigest()[:8]
    fetched_at = datetime.now(timezone.utc)  # always UTC
    stats      = {"jobs_fetched": 0, "jobs_inserted": 0, "jobs_skipped": 0}

    try:
        jobs = search_jobs(query)
        stats["jobs_fetched"] = len(jobs)

        for job in jobs:
            job_id = job.get("job_id")
            if not job_id:
                continue

            # Idempotency check — explicit select before insert
            # Also lets us count skipped rows for the audit log
            exists = con.execute(
                "SELECT 1 FROM raw.job_postings WHERE job_id = ?", [job_id]
            ).fetchone()

            if exists:
                stats["jobs_skipped"] += 1
                continue

            con.execute(
                """
                INSERT INTO raw.job_postings
                    (job_id, search_query, fetched_at, raw_json)
                VALUES (?, ?, ?, ?)
                """,
                [job_id, query, fetched_at, json.dumps(job)],
            )
            stats["jobs_inserted"] += 1

        status    = "success"
        error_msg = None

    except Exception as exc:
        log.error(f"  Ingestion failed for query={query!r}: {exc}")
        status    = "error"
        error_msg = str(exc)

    # Audit log — always write, even on error
    # In Step 4 (orchestration with Prefect/Airflow), you'll query this
    # table to detect missed runs, zero-row fetches, and error patterns.
    con.execute(
        "INSERT INTO raw.ingest_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run_id, query, fetched_at,
            stats["jobs_fetched"],
            stats["jobs_inserted"],
            stats["jobs_skipped"],
            status, error_msg,
        ],
    )

    log.info(
        f"  [{query}]  "
        f"fetched={stats['jobs_fetched']}  "
        f"inserted={stats['jobs_inserted']}  "
        f"skipped={stats['jobs_skipped']}"
    )
    return stats


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    if not RAPID_KEY:
        raise EnvironmentError(
            "RAPID_KEY not set.\n"
            "Create a .env file with: RAPID_KEY=your_key_here\n"
            "Get a free key at: https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch"
        )

    con = get_connection()
    bootstrap_schema(con)

    total = {"jobs_fetched": 0, "jobs_inserted": 0, "jobs_skipped": 0}

    for query in SEARCH_QUERIES:
        log.info(f"-- Ingesting: {query!r}")
        stats = ingest_query(con, query)
        for k in total:
            total[k] += stats[k]
        time.sleep(1)  # polite pause between queries

    log.info("-- Run complete")
    log.info(f"   Total fetched  : {total['jobs_fetched']}")
    log.info(f"   Total inserted : {total['jobs_inserted']}")
    log.info(f"   Total skipped  : {total['jobs_skipped']} (already in warehouse)")
    log.info(f"   Warehouse      : {DB_PATH}")

    count = con.execute("SELECT COUNT(*) FROM raw.job_postings").fetchone()[0]
    log.info(f"   Warehouse total: {count} job postings")

    con.close()


if __name__ == "__main__":
    main()