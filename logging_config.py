import logging
import os

LOG_DIR = "logs"
LOG_FILE = os.path.join(
    LOG_DIR,
    "ingest_jobs_pipeline.log"
)

os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging():

    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    return logging.getLogger("job_analytics")