import json
import uuid

from datetime import (
    datetime,
    timezone
)

from clients.jsearch_client import (
    JSearchClient
)


def ingest_query(
    con,
    query,
    logger
):

    client = JSearchClient()

    run_id = str(uuid.uuid4())

    fetched_at = datetime.now(
        timezone.utc
    )

    stats = {
        "jobs_fetched": 0,
        "jobs_inserted": 0,
        "jobs_skipped": 0,
        "error_msg": None
    }

    status = "success"

    try:

        jobs = client.search_jobs(query)

        stats["jobs_fetched"] = len(jobs)

        rows = []

        for job in jobs:

            job_id = job.get("job_id")

            if not job_id:
                continue

            details = (
                client.get_job_details(job_id)
                or {}
            )

            rows.append(
                (
                    job_id,
                    query,
                    fetched_at,
                    run_id,
                    json.dumps(details)
                )
            )

        for row in rows:

            before = con.execute(
                """
                SELECT COUNT(*)
                FROM raw.job_postings
                """
            ).fetchone()[0]

            con.execute(
                """
                INSERT INTO raw.job_postings
                (
                    job_id,
                    search_query,
                    fetched_at,
                    ingestion_run_id,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id)
                DO NOTHING
                """,
                row
            )

            after = con.execute(
                """
                SELECT COUNT(*)
                FROM raw.job_postings
                """
            ).fetchone()[0]

            if after > before:
                stats["jobs_inserted"] += 1
            else:
                stats["jobs_skipped"] += 1

    except Exception as e:

        status = "failed"

        stats["error_msg"] = str(e)

        logger.exception(
            "Failed ingestion for %s",
            query
        )

    finally:

        con.execute(
            """
            INSERT INTO raw.ingest_log
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                query,
                fetched_at,
                stats["jobs_fetched"],
                stats["jobs_inserted"],
                stats["jobs_skipped"],
                status,
                stats["error_msg"]
            )
        )

        return stats