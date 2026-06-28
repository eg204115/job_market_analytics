from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

import requests


api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(
        multiplier=2,
        min=2,
        max=30
    ),
    retry=retry_if_exception_type(
        requests.RequestException
    ),
    reraise=True
)