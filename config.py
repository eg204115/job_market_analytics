from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    rapid_key: str
    rapid_host: str
    db_path: str
    page_size: int

    @classmethod
    def load(cls):
        return cls(
            rapid_key=os.getenv("RAPID_KEY"),
            rapid_host=os.getenv(
                "RAPID_HOST",
                "jsearch.p.rapidapi.com"
            ),
            db_path=os.getenv(
                "DB_PATH",
                "jobs_warehouse.duckdb"
            ),
            page_size=int(
                os.getenv("PAGE_SIZE", 50)
            )
        )


config = Config.load()