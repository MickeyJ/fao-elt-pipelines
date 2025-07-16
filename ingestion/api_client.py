"""FAO API Client for fetching data from endpoints."""

import logging
import time
from urllib.parse import urljoin

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAOApiClient:
    """Client for interacting with FAO API endpoints."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()

    def fetch_data(self, endpoint: str, max_pages: int = 5) -> list[dict]:
        """
        Fetch data from API with pagination support.

        Args:
            endpoint: API endpoint path
            max_pages: Maximum number of pages to fetch (for demo purposes)

        Returns:
            List of all data records
        """
        all_data = []
        page = 1

        # Build full URL
        url = endpoint if endpoint.startswith("http") else urljoin(self.base_url, endpoint)

        while page <= max_pages:
            try:
                logger.info(f"Fetching page {page} from {url}")
                response = self.session.get(url)
                response.raise_for_status()

                result = response.json()

                # Extract data
                if "data" in result:
                    all_data.extend(result["data"])

                # Check for next page
                if "links" in result and "next" in result["links"] and result["links"]["next"]:
                    url = result["links"]["next"]
                    page += 1
                    # Be nice to the API
                    time.sleep(0.5)
                else:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from {url}: {e}")
                break

        logger.info(f"Fetched {len(all_data)} records in total")
        return all_data

    def fetch_prices_data(self, max_pages: int = 3) -> list[dict]:
        """Fetch producer prices data."""
        endpoint = "/prices/prices/?element_code=5532&element=&flag=A&sort=year%2Citem_code"
        return self.fetch_data(endpoint, max_pages)

    def fetch_food_balance_data(self, max_pages: int = 3) -> list[dict]:
        """Fetch food balance sheets data."""
        endpoint = "/food/food_balance_sheets/?element_code=5511&flag=&sort=year%2Citem_code"
        return self.fetch_data(endpoint, max_pages)
