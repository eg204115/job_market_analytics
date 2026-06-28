import requests

from config import config
from utils.retry import api_retry


class JSearchClient:

    def __init__(self):

        self.base_url = (
            f"https://{config.rapid_host}"
        )

        self.session = requests.Session()

        self.headers = {
            "X-RapidAPI-Key": config.rapid_key,
            "X-RapidAPI-Host": config.rapid_host
        }

    @api_retry
    def search_jobs(self, query: str):

        response = self.session.get(
            f"{self.base_url}/search",
            headers=self.headers,
            params={
                "query": query,
                "page": 1,
                "date_posted": "week",
                "num_pages": config.page_size
            },
            timeout=15
        )

        response.raise_for_status()

        return response.json().get(
            "data",
            []
        )

    @api_retry
    def get_job_details(
        self,
        job_id: str
    ):

        response = self.session.get(
            f"{self.base_url}/job-details",
            headers=self.headers,
            params={
                "job_id": job_id
            },
            timeout=15
        )

        response.raise_for_status()

        data = response.json().get(
            "data",
            []
        )

        return data[0] if data else {}