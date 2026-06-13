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

"""

import os
import json
import time
import logging
import hashlib
import requests
import duckdb
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()
RAPID_KEY = os.getenv('RAPID_KEY')
RAPID_HOST = os.getenv('RAPID_HOST')
DB_PATH = 'jobs_warehouse.duckdb'
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'ingest_jobs_pipeline.log')

SEARCH_QUERIES = ["data engineer", "data scientist", "machine learning engineer"]
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True,
)
logging.info("Starting job ingestion process")
log = logging.getLogger(__name__)

def get_connection() -> duckdb.DuckDBPyConnection:
    """Establishes a connection to the DuckDB database."""
    return duckdb.connect(DB_PATH)

def bootstrap_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Creates the necessary schema and tables if they don't exist.
    aw.job_postings  — landing zone: one row per job, full JSON blob
    raw.ingest_log    — audit log: one row per pipeline run per query
    """

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")  # namespace for raw data tables
    con.execute("""
CREATE TABLE IF NOT EXISTS raw.job_postings (
    job_id VARCHAR PRIMARY KEY,
    search_query VARCHAR,
    fetched_at TIMESTAMPTZ,
    raw_json JSON
)
                """)
    con.execute("""
CREATE TABLE IF NOT EXISTS raw.ingest_log (
    run_id VARCHAR ,
    search_query VARCHAR,
    fetched_at TIMESTAMPTZ,
    jobs_fetched INTEGER,
    jobs_inserted INTEGER,
    jobs_skipped INTEGER,
    status VARCHAR,
    error_msg VARCHAR
)
                """)
    
    # create staging and mart schemas for future transformations
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")  
    con.execute("CREATE SCHEMA IF NOT EXISTS mart")

    log.info("Database schema bootstrapped successfully")

    # API client

def search_jobs(query: str)-> list[dict]:
    """Fetches job postings from the API for a given search query."""
    url = f"https://{RAPID_HOST}/search"
    headers = {
        "X-RapidAPI-Key": RAPID_KEY,
        "X-RapidAPI-Host": RAPID_HOST
    }
    params = {
        "query": query,
        "page": 1,
        "date_posted": "week",  # fetch only recent jobs to avoid duplicates
        "num_pages": 1          # limit to first page for demo; can be increased
    }
    log.info(f"Fetching jobs for query '{query!r}' from API") 
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        jobs= data.get("data", [])
        log.info(f"Retrieved {len(jobs)} jobs for query '{query!r}'")
        return jobs  # return list of job dicts
    except requests.RequestException as e:
        log.error(f"API request failed for query '{query!r}': {e}")
        return []
    
def get_job_details(job_id: str) -> dict |None :
    """Fetches detailed job information for a given job ID."""
    url = f"https://{RAPID_HOST}/job-details?job_id={job_id}"
    headers = {
        "X-RapidAPI-Key": RAPID_KEY,
        "X-RapidAPI-Host": RAPID_HOST
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json().get("data", [None])[0]  # return full job details as dict
    except requests.RequestException as e:
        log.error(f"Failed to fetch details for job_id '{job_id}': {e}")
        return {}
    
# Ingestion logic

def ingest_query(con: duckdb.DuckDBPyConnection, query: str) -> dict:
    """
    Pull jobs for one search query and upsert into raw.job_postings.
 
    Idempotency: checking job_id before insert means this function is safe
    to run multiple times. Re-running produces the same warehouse state —
    a fundamental property of reliable data pipelines.
    """
    run_id = hashlib.md5(f"{query}_{datetime.now()}".encode()).hexdigest()[:8]
    fetched_at = datetime.now(timezone.utc) #always UTC
    stats = {"jobs_fetched": 0, "jobs_inserted": 0, "jobs_skipped": 0, "error_msg": None}

    try:
        jobs = search_jobs(query)
        stats["jobs_fetched"] = len(jobs)

        for job in jobs:
            job_id = job.get("job_id")
            if not job_id:
                log.warning(f"Skipping job with missing job_id: {job}")
                continue

            # Check if job_id already exists
            existing = con.execute("SELECT 1 FROM raw.job_postings WHERE job_id = ?", [job_id]).fetchone()
            if existing:
                stats["jobs_skipped"] += 1
                continue  # skip duplicates

            # Fetch full job details for richer data in the warehouse
            full_details = get_job_details(job_id) or {}
            con.execute("""
                INSERT INTO raw.job_postings (job_id, search_query, fetched_at, raw_json)
                VALUES (?, ?, ?, ?)
            """, (job_id, query, fetched_at, json.dumps(full_details)))
            stats["jobs_inserted"] += 1
        status = "success"
    except Exception as e:
        status = "failed"
        stats["error_msg"] = str(e)
        log.error(f"Error occurred while ingesting query '{query!r}': {e}")
    finally:
        # Write audit log entry regardless of success or failure
        con.execute("""
            INSERT INTO raw.ingest_log (run_id, search_query, fetched_at, jobs_fetched, jobs_inserted, jobs_skipped, status, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, query, fetched_at, stats["jobs_fetched"], stats["jobs_inserted"], stats["jobs_skipped"], status, stats["error_msg"]))
        log.info(f"Ingestion completed for query '{query!r}': {stats}")
        return stats
    
def main() -> None:
    if not RAPID_KEY or not RAPID_HOST:
        raise EnvironmentError("RAPID_KEY and RAPID_HOST must be set in environment variables")

    con = get_connection()
    try:
        bootstrap_schema(con)
        total = {"jobs_fetched": 0, "jobs_inserted": 0, "jobs_skipped": 0}
        for query in SEARCH_QUERIES:
            log.info(f"Starting ingestion for query: {query!r}")
            stats = ingest_query(con, query)
            for key in total:
                total[key] += stats.get(key, 0)
            time.sleep(1)  # brief pause between queries to respect API rate limits
        log.info(f"All queries completed. Total stats: {total}")
        log.info("Total inserted: %d, Total skipped: %d, Total fetched: %d", total["jobs_inserted"], total["jobs_skipped"], total["jobs_fetched"])
        log.info(f"Warehouse: {DB_PATH}")
        count = con.execute("SELECT COUNT(*) FROM raw.job_postings").fetchone()[0]
        log.info(f"Total unique jobs in warehouse: {count}")
    finally:
        con.close()

if __name__ == "__main__":    main()
    