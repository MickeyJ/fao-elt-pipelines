"""Improved FAO API Client with comprehensive error handling, retry logic, and pagination."""

import logging
import time
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAOApiClient:
    """
    Enhanced FAO API Client with robust error handling, retry mechanisms, and pagination support.
    """

    def __init__(self, base_url: str, default_timeout: int = 30, rate_limit_delay: float = 0.5):
        """
        Initialize the FAO API client.

        Args:
            base_url: Base URL for the FAO API
            default_timeout: Default timeout for requests in seconds
            rate_limit_delay: Delay between requests to be nice to the API
        """
        self.base_url = base_url.rstrip("/")
        self.default_timeout = default_timeout
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update(
            {
                "User-Agent": "FAO-API-Client/1.0",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def test_connectivity(self) -> tuple[bool, str]:
        """
        Test if the API is accessible.

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            test_url = f"{self.base_url}"
            logger.info(f"Testing connectivity to: {test_url}")

            response = self.session.get(test_url, timeout=self.default_timeout)
            response.raise_for_status()

            data = response.json()

            # Handle different response structures
            record_count = 0
            if isinstance(data, dict):
                if "data" in data:
                    record_count = len(data["data"])
                elif "results" in data:
                    record_count = len(data["results"])
            elif isinstance(data, list):
                record_count = len(data)

            if record_count > 0:
                return True, f"API accessible. Test returned {record_count} records."
            else:
                return True, "API accessible but no data in test response."

        except requests.exceptions.Timeout:
            return (
                False,
                f"API connectivity test failed: Request timed out after {self.default_timeout}s",
            )
        except requests.exceptions.ConnectionError:
            return False, "API connectivity test failed: Unable to connect to the server"
        except requests.exceptions.HTTPError as e:
            return False, f"API connectivity test failed: HTTP {e.response.status_code} error"
        except requests.exceptions.RequestException as e:
            return False, f"API connectivity test failed: {e!s}"
        except Exception as e:
            return False, f"Unexpected error during connectivity test: {e!s}"

    def fetch_data(
        self,
        endpoint: str,
        limit: int = 500,
        max_pages: int = 5,
        additional_params: dict[str, str] | None = None,
    ) -> tuple[list[dict], dict]:
        """
        Fetch data from API endpoint with pagination support.

        Args:
            endpoint: API endpoint path (can include query parameters)
            limit: Number of records per page
            max_pages: Maximum number of pages to fetch
            additional_params: Additional query parameters to add

        Returns:
            Tuple[List[Dict], Dict]: (all_data_records, metadata)
        """
        metadata = {
            "endpoint": endpoint,
            "limit": limit,
            "max_pages": max_pages,
            "pages_fetched": 0,
            "total_records": 0,
            "errors": [],
            "warnings": [],
            "sample_record": None,
            "pagination_info": {},
            "fetch_duration": 0,
        }

        start_time = time.time()
        all_data = []
        page = 1

        # Build initial URL
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"

        # Add limit parameter
        separator = "&" if "?" in url else "?"
        url += f"{separator}limit={limit}"

        # Add additional parameters if provided
        if additional_params:
            for key, value in additional_params.items():
                url += f"&{key}={value}"

        logger.info(f"Starting data fetch from: {url}")
        logger.info(f"Max pages: {max_pages}, Limit per page: {limit}")

        while page <= max_pages:
            try:
                logger.info(f"🔄 Fetching page {page}/{max_pages} from: {url}")

                response = self.session.get(url, timeout=self.default_timeout)
                response.raise_for_status()

                result = response.json()
                page_data = []

                # Extract data from different response structures
                if isinstance(result, dict):
                    if "data" in result:
                        page_data = result["data"]
                    elif "results" in result:
                        page_data = result["results"]
                    else:
                        # Check if the entire result is the data
                        if all(isinstance(v, dict | list) for v in result.values()):
                            warning_msg = f"Unexpected response structure on page {page}. Keys: {list(result.keys())}"
                            metadata["warnings"].append(warning_msg)
                            logger.warning(warning_msg)
                elif isinstance(result, list):
                    page_data = result

                if page_data:
                    all_data.extend(page_data)
                    logger.info(f"✅ Page {page}: Fetched {len(page_data)} records")

                    # Store sample record from first page
                    if page == 1 and page_data:
                        metadata["sample_record"] = page_data[0]
                        logger.info(
                            f"📄 Sample record keys: {list(page_data[0].keys()) if isinstance(page_data[0], dict) else 'Not a dict'}"
                        )
                else:
                    warning_msg = f"No data found on page {page}"
                    metadata["warnings"].append(warning_msg)
                    logger.warning(warning_msg)

                metadata["pages_fetched"] = page

                # Check for pagination links
                next_url = None
                has_next_page = False

                if isinstance(result, dict):
                    # First check if pagination object explicitly says there's a next page
                    if "pagination" in result and isinstance(result["pagination"], dict):
                        has_next_page = result["pagination"].get("has_next", False)

                    # Only look for next URL if has_next is True (or pagination info is missing)
                    if has_next_page or "pagination" not in result:
                        # Check for different pagination patterns
                        if "links" in result and isinstance(result["links"], dict):
                            next_url = result["links"].get("next")
                        elif "next" in result:
                            next_url = result["next"]
                        elif "_links" in result:
                            next_url = result["_links"].get("next", {}).get("href")

                if next_url and page < max_pages:
                    url = (
                        next_url
                        if next_url.startswith("http")
                        else urljoin(self.base_url, next_url)
                    )
                    page += 1

                    # Rate limiting
                    if self.rate_limit_delay > 0:
                        logger.debug(f"Rate limiting: sleeping for {self.rate_limit_delay}s")
                        time.sleep(self.rate_limit_delay)
                else:
                    if page >= max_pages:
                        logger.info(f"Reached maximum pages limit ({max_pages})")
                    elif not has_next_page and "pagination" in result:
                        logger.info("No more pages available (pagination.has_next = false)")
                    else:
                        logger.info("No more pages available")
                    break

            except requests.exceptions.Timeout:
                error_msg = f"Request timeout on page {page} after {self.default_timeout}s"
                metadata["errors"].append(error_msg)
                logger.error(error_msg)
                break
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP {e.response.status_code} error on page {page}: {e!s}"
                metadata["errors"].append(error_msg)
                logger.error(error_msg)
                break
            except requests.exceptions.RequestException as e:
                error_msg = f"Request error on page {page}: {e!s}"
                metadata["errors"].append(error_msg)
                logger.error(error_msg)
                break
            except Exception as e:
                error_msg = f"Unexpected error on page {page}: {e!s}"
                metadata["errors"].append(error_msg)
                logger.error(error_msg)
                break

        # Finalize metadata
        metadata["total_records"] = len(all_data)
        metadata["fetch_duration"] = round(time.time() - start_time, 2)
        metadata["pagination_info"] = {
            "pages_requested": max_pages,
            "pages_fetched": metadata["pages_fetched"],
            "records_per_page_avg": round(len(all_data) / max(metadata["pages_fetched"], 1), 2),
        }

        logger.info(
            f"🎯 Fetch complete: {len(all_data)} total records from {metadata['pages_fetched']} pages in {metadata['fetch_duration']}s"
        )

        if metadata["errors"]:
            logger.warning(f"⚠️  Encountered {len(metadata['errors'])} errors during fetch")

        return all_data, metadata

    def fetch_prices_data(
        self, limit: int = 500, max_pages: int = 5, element_code: str = "5532", flag: str = "A"
    ) -> tuple[list[dict], dict]:
        """
        Fetch producer prices data with specific query parameters.

        Args:
            limit: Number of records per page
            max_pages: Maximum pages to fetch
            element_code: Element code for producer prices (default: 5532)
            flag: Flag parameter (default: A)

        Returns:
            Tuple[List[Dict], Dict]: (data_records, metadata)
        """
        endpoint = (
            f"prices/prices/?element_code={element_code}&element=&flag={flag}&sort=year%2Citem_code"
        )
        return self.fetch_data(endpoint, limit, max_pages)

    def fetch_food_balance_data(
        self, limit: int = 500, max_pages: int = 5, element_code: str = "5511"
    ) -> tuple[list[dict], dict]:
        """
        Fetch food balance sheets data with specific query parameters.

        Args:
            limit: Number of records per page
            max_pages: Maximum pages to fetch
            element_code: Element code for food balance (default: 5511)

        Returns:
            Tuple[List[Dict], Dict]: (data_records, metadata)
        """
        endpoint = (
            f"food/food_balance_sheets/?element_code={element_code}&flag=&sort=year%2Citem_code"
        )
        return self.fetch_data(endpoint, limit, max_pages)

    def fetch_custom_endpoint(
        self,
        endpoint_path: str,
        params: dict[str, str] | None = None,
        limit: int = 500,
        max_pages: int = 5,
    ) -> tuple[list[dict], dict]:
        """
        Fetch data from a custom endpoint with custom parameters.

        Args:
            endpoint_path: The endpoint path (e.g., "prices/prices")
            params: Dictionary of query parameters
            limit: Number of records per page
            max_pages: Maximum pages to fetch

        Returns:
            Tuple[List[Dict], Dict]: (data_records, metadata)
        """
        # Build endpoint with parameters
        if params:
            param_string = "&".join([f"{k}={v}" for k, v in params.items()])
            endpoint = f"{endpoint_path}?{param_string}"
        else:
            endpoint = endpoint_path

        return self.fetch_data(endpoint, limit, max_pages)

    def get_metadata_summary(self, metadata: dict) -> str:
        """
        Generate a human-readable summary of fetch metadata.

        Args:
            metadata: Metadata dictionary from fetch operation

        Returns:
            Formatted summary string
        """
        summary = [
            f"📊 Fetch Summary for {metadata['endpoint']}",
            f"Records: {metadata['total_records']:,}",
            f"Pages: {metadata['pages_fetched']}/{metadata['max_pages']}",
            f"Duration: {metadata['fetch_duration']}s",
        ]

        if metadata["errors"]:
            summary.append(f"❌ Errors: {len(metadata['errors'])}")

        if metadata["warnings"]:
            summary.append(f"⚠️  Warnings: {len(metadata['warnings'])}")

        return "\n".join(summary)
