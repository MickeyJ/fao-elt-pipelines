"""Main ELT pipeline orchestration using Prefect."""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from ingestion.api_client import FAOApiClient
from ingestion.load_to_database import PostgresLoader

load_dotenv(override=True)

# Configuration
API_BASE_URL = os.getenv("FAO_API_BASE_URL", "")
PRICES_ENDPOINT = os.getenv("PRICES_ENDPOINT", "")
FOOD_BALANCE_ENDPOINT = os.getenv("FOOD_BALANCE_ENDPOINT", "")
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt_project"
ALLOWED_DBT_TARGETS = ["dev", "prod", "test"]


def validate_dbt_target(target: str) -> None:
    """Validate dbt target to prevent command injection."""
    if target not in ALLOWED_DBT_TARGETS:
        raise ValueError(f"Invalid target '{target}'. Must be one of: {ALLOWED_DBT_TARGETS}")


def log_extraction_metadata(logger, metadata: dict, data_type: str) -> None:
    """Log detailed extraction metadata for monitoring."""
    logger.info(f"=== {data_type} Extraction Metadata ===")
    logger.info(f"Total records: {metadata.get('total_records', 0):,}")
    logger.info(f"Pages fetched: {metadata.get('pages_fetched', 0)}/{metadata.get('max_pages', 0)}")
    logger.info(f"Fetch duration: {metadata.get('fetch_duration', 0)}s")

    if metadata.get("errors"):
        logger.warning(f"Errors encountered: {len(metadata['errors'])}")
        for error in metadata["errors"]:
            logger.warning(f"  - {error}")

    if metadata.get("warnings"):
        logger.warning(f"Warnings: {len(metadata['warnings'])}")
        for warning in metadata["warnings"]:
            logger.warning(f"  - {warning}")

    pagination_info = metadata.get("pagination_info", {})
    if pagination_info:
        logger.info(f"Average records per page: {pagination_info.get('records_per_page_avg', 0)}")


@task(name="Test API Connectivity", retries=2, retry_delay_seconds=10)
def test_api_connectivity() -> bool:
    """Test API connectivity before starting extraction."""
    logger = get_run_logger()
    logger.info("Testing FAO API connectivity")

    client = FAOApiClient(API_BASE_URL)
    is_connected, message = client.test_connectivity()

    if is_connected:
        logger.info(f"✅ API connectivity test passed: {message}")
        return True
    else:
        logger.error(f"❌ API connectivity test failed: {message}")
        raise Exception(f"API connectivity test failed: {message}")


@task(name="Extract Prices Data", retries=3, retry_delay_seconds=30)
def extract_prices_data(max_pages: int = 3, limit: int = 500) -> tuple[list[dict], dict]:
    """Extract prices data from FAO API."""
    logger = get_run_logger()
    logger.info(f"Starting prices data extraction (max_pages={max_pages}, limit={limit})")

    client = FAOApiClient(API_BASE_URL)

    # Use the convenience method for prices data if PRICES_ENDPOINT is standard,
    # otherwise use the custom endpoint method
    if PRICES_ENDPOINT and ("prices" in PRICES_ENDPOINT.lower()):
        # If PRICES_ENDPOINT contains full path with parameters, use fetch_data
        data, metadata = client.fetch_data(
            endpoint=PRICES_ENDPOINT, limit=limit, max_pages=max_pages
        )
    else:
        # Use the convenience method with default parameters
        data, metadata = client.fetch_prices_data(limit=limit, max_pages=max_pages)

    log_extraction_metadata(logger, metadata, "Prices")
    logger.info(f"✅ Extracted {len(data)} price records")

    # Return both data and metadata for potential use downstream
    return data, metadata


@task(name="Extract Food Balance Data", retries=3, retry_delay_seconds=30)
def extract_food_balance_data(max_pages: int = 3, limit: int = 500) -> tuple[list[dict], dict]:
    """Extract food balance sheets data from FAO API."""
    logger = get_run_logger()
    logger.info(f"Starting food balance data extraction (max_pages={max_pages}, limit={limit})")

    client = FAOApiClient(API_BASE_URL)

    # Use the convenience method for food balance data if FOOD_BALANCE_ENDPOINT is standard,
    # otherwise use the custom endpoint method
    if FOOD_BALANCE_ENDPOINT and ("food" in FOOD_BALANCE_ENDPOINT.lower()):
        # If FOOD_BALANCE_ENDPOINT contains full path with parameters, use fetch_data
        data, metadata = client.fetch_data(
            endpoint=FOOD_BALANCE_ENDPOINT, limit=limit, max_pages=max_pages
        )
    else:
        # Use the convenience method with default parameters
        data, metadata = client.fetch_food_balance_data(limit=limit, max_pages=max_pages)

    log_extraction_metadata(logger, metadata, "Food Balance")
    logger.info(f"✅ Extracted {len(data)} food balance records")

    # Return both data and metadata for potential use downstream
    return data, metadata


@task(name="Load to Bronze Layer")
def load_to_bronze(
    prices_result: tuple[list[dict], dict],
    food_balance_result: tuple[list[dict], dict],
    truncate_first: bool = True,
):
    """Load raw data to PostgreSQL bronze layer."""
    logger = get_run_logger()
    loader = PostgresLoader()

    # Extract data from the tuples
    prices_data, prices_metadata = prices_result
    food_balance_data, food_balance_metadata = food_balance_result

    if truncate_first:
        logger.info("Truncating bronze tables")
        loader.truncate_bronze_tables()

    # Load prices data
    logger.info(f"Loading {len(prices_data)} prices records to bronze layer")
    loader.load_to_bronze(
        data=prices_data,
        table_name="raw_prices",
        source_url=API_BASE_URL,
        api_endpoint=prices_metadata.get("endpoint", "/prices/prices"),
    )

    # Load food balance data
    logger.info(f"Loading {len(food_balance_data)} food balance records to bronze layer")
    loader.load_to_bronze(
        data=food_balance_data,
        table_name="raw_food_balance",
        source_url=API_BASE_URL,
        api_endpoint=food_balance_metadata.get("endpoint", "/food/food_balance_sheets"),
    )

    logger.info("✅ Bronze layer loading complete")

    # Log loading summary
    total_records = len(prices_data) + len(food_balance_data)
    logger.info(
        f"📊 Total records loaded: {total_records:,} (Prices: {len(prices_data):,}, Food Balance: {len(food_balance_data):,})"
    )


@task(name="Run dbt Models")
def run_dbt_transformations(target: str = "dev"):
    """Run dbt models to transform bronze → silver → gold."""
    logger = get_run_logger()

    # Validate target parameter to prevent injection
    validate_dbt_target(target)

    logger.info(f"Starting dbt transformations (target={target})")

    # Run dbt models using subprocess
    result = subprocess.run(
        ["dbt", "run", "--target", target],
        cwd=str(DBT_PROJECT_PATH),
        capture_output=True,
        text=True,
    )

    # Log the output
    if result.stdout:
        logger.info(f"dbt output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"❌ dbt run failed:\n{result.stderr}")
        raise Exception(f"dbt run failed with return code {result.returncode}")

    logger.info("✅ dbt transformations complete")
    return result.stdout


@task(name="Run dbt Tests")
def run_dbt_tests(target: str = "dev"):
    """Run dbt tests for data quality validation."""
    logger = get_run_logger()

    # Validate target parameter to prevent injection
    validate_dbt_target(target)

    logger.info("Running dbt tests")

    result = subprocess.run(
        ["dbt", "test", "--target", target],
        cwd=str(DBT_PROJECT_PATH),
        capture_output=True,
        text=True,
    )

    # Log the output
    if result.stdout:
        logger.info(f"dbt test output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"❌ dbt tests failed:\n{result.stderr}")
        raise Exception(f"dbt tests failed with return code {result.returncode}")

    logger.info("✅ dbt tests complete")
    return result.stdout


@task(name="Generate dbt Documentation")
def generate_dbt_docs(target: str = "dev"):
    """Generate dbt documentation."""
    logger = get_run_logger()

    # Validate target parameter to prevent injection
    validate_dbt_target(target)

    logger.info("Generating dbt documentation")

    result = subprocess.run(
        ["dbt", "docs", "generate", "--target", target],
        cwd=str(DBT_PROJECT_PATH),
        capture_output=True,
        text=True,
    )

    # Log the output
    if result.stdout:
        logger.info(f"dbt docs output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"❌ dbt docs generation failed:\n{result.stderr}")
        raise Exception(f"dbt docs generation failed with return code {result.returncode}")

    logger.info("✅ dbt documentation generated")
    return result.stdout


@flow(
    name="FAO ELT Pipeline",
    description="Extract FAO data, Load to Bronze, Transform through Silver to Gold",
)
def fao_elt_pipeline(
    max_pages_prices: int = 3,
    max_pages_food_balance: int = 3,
    limit_per_page: int = 500,
    truncate_bronze: bool = True,
    run_tests: bool = True,
    generate_docs: bool = False,
    skip_connectivity_test: bool = False,
):
    """
    Main ELT pipeline flow.

    Args:
        max_pages_prices: Maximum pages to fetch from prices API
        max_pages_food_balance: Maximum pages to fetch from food balance API
        limit_per_page: Records per page for both APIs
        truncate_bronze: Whether to truncate bronze tables before loading
        run_tests: Whether to run dbt tests after transformations
        generate_docs: Whether to generate dbt documentation
        skip_connectivity_test: Skip API connectivity test (for faster local testing)
    """
    logger = get_run_logger()
    logger.info("🚀 Starting FAO ELT Pipeline")
    logger.info(
        f"Configuration: prices_pages={max_pages_prices}, food_balance_pages={max_pages_food_balance}, limit={limit_per_page}"
    )

    # Optional connectivity test
    if not skip_connectivity_test:
        test_api_connectivity()

    # Extract phase
    logger.info("📥 Starting extraction phase")
    prices_result = extract_prices_data(max_pages=max_pages_prices, limit=limit_per_page)
    food_balance_result = extract_food_balance_data(
        max_pages=max_pages_food_balance, limit=limit_per_page
    )

    # Load phase (to Bronze)
    logger.info("💾 Starting load phase")
    load_to_bronze(
        prices_result=prices_result,
        food_balance_result=food_balance_result,
        truncate_first=truncate_bronze,
    )

    # Transform phase (Bronze → Silver → Gold)
    logger.info("🔄 Starting transform phase")
    dbt_result = run_dbt_transformations()
    logger.info(f"dbt transformation result preview: {dbt_result[:200]}...")

    # Optional: Run tests
    if run_tests:
        logger.info("🧪 Running data quality tests")
        test_result = run_dbt_tests()
        logger.info(f"dbt test result preview: {test_result[:200]}...")

    # Optional: Generate documentation
    if generate_docs:
        logger.info("📚 Generating documentation")
        docs_result = generate_dbt_docs()
        logger.info(f"dbt docs result preview: {docs_result[:200]}...")

    logger.info("🎉 FAO ELT Pipeline completed successfully")

    # Return summary statistics
    prices_data, prices_metadata = prices_result
    food_balance_data, food_balance_metadata = food_balance_result

    summary = {
        "prices_records": len(prices_data),
        "food_balance_records": len(food_balance_data),
        "total_records": len(prices_data) + len(food_balance_data),
        "prices_pages_fetched": prices_metadata.get("pages_fetched", 0),
        "food_balance_pages_fetched": food_balance_metadata.get("pages_fetched", 0),
        "total_fetch_duration": prices_metadata.get("fetch_duration", 0)
        + food_balance_metadata.get("fetch_duration", 0),
    }

    logger.info(f"📊 Pipeline Summary: {summary}")
    return summary


if __name__ == "__main__":
    # Run the pipeline with default settings
    result = fao_elt_pipeline(
        max_pages_prices=10,  # Fetch 2 pages for demo
        max_pages_food_balance=10,
        limit_per_page=500,  # 500 records per page
        truncate_bronze=True,
        run_tests=True,
        generate_docs=True,
        skip_connectivity_test=False,  # Test connectivity in production
    )
    print("Pipeline completed with result:", result)
