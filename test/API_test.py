import os
import requests
from dotenv import load_dotenv

load_dotenv()

RAPID_KEY = os.getenv("RAPID_KEY")
RAPID_HOST = os.getenv("RAPID_HOST", "jsearch.p.rapidapi.com")

headers = {
    "X-RapidAPI-Key": RAPID_KEY,
    "X-RapidAPI-Host": RAPID_HOST,
}


def test_search():
    print("\n=== TESTING SEARCH API ===")

    response = requests.get(
        f"https://{RAPID_HOST}/search",
        headers=headers,
        params={
            "query": "data engineer",
            "page": 1,
            "num_pages": 1,
            "date_posted": "week",
        },
        timeout=30,
    )

    print(f"Status Code: {response.status_code}")

    response.raise_for_status()

    data = response.json()

    jobs = data.get("data", [])

    print(f"Jobs returned: {len(jobs)}")

    if jobs:
        print(f"First Job ID: {jobs[0].get('job_id')}")

    return jobs


def test_job_details(job_id):
    print("\n=== TESTING JOB DETAILS API ===")
    print(f"Job ID: {job_id}")

    response = requests.get(
        f"https://{RAPID_HOST}/job-details",
        headers=headers,
        params={"job_id": job_id},
        timeout=30,
    )

    print(f"Status Code: {response.status_code}")

    response.raise_for_status()

    data = response.json()

    details = data.get("data", [])

    print(f"Records returned: {len(details)}")

    if details:
        print("Job Title:", details[0].get("job_title"))
        print("Company:", details[0].get("employer_name"))

    return details


if __name__ == "__main__":

    jobs = test_search()

    if jobs:
        job_id = jobs[0]["job_id"]
        test_job_details(job_id)

    print("\nAPI TEST COMPLETED SUCCESSFULLY")