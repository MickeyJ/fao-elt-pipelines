"""Main ELT pipeline orchestration using Prefect."""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from ingestion.api_client import FAOApiClient
from ingestion.load_to_database import PostgresLoader

load_dotenv()

# Configuration
API_BASE_URL = os.getenv("FAO_API_BASE_URL", "https://kw2aqt7p3p.us-west-2.awsapprunner.com/v1")
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt_project"


@task(name="Extract Prices Data", retries=3, retry_delay_seconds=30)
def extract_prices_data(max_pages: int = 3) -> list:
    """Extract prices data from FAO API."""
    logger = get_run_logger()
    logger.info(f"Starting prices data extraction (max_pages={max_pages})")

    client = FAOApiClient(API_BASE_URL)
    data = client.fetch_prices_data(max_pages=max_pages)

    logger.info(f"Extracted {len(data)} price records")
    return data


@task(name="Extract Food Balance Data", retries=3, retry_delay_seconds=30)
def extract_food_balance_data(max_pages: int = 3) -> list:
    """Extract food balance sheets data from FAO API."""
    logger = get_run_logger()
    logger.info(f"Starting food balance data extraction (max_pages={max_pages})")

    client = FAOApiClient(API_BASE_URL)
    data = client.fetch_food_balance_data(max_pages=max_pages)

    logger.info(f"Extracted {len(data)} food balance records")
    return data


@task(name="Load to Bronze Layer")
def load_to_bronze(prices_data: list, food_balance_data: list, truncate_first: bool = True):
    """Load raw data to PostgreSQL bronze layer."""
    logger = get_run_logger()
    loader = PostgresLoader()

    if truncate_first:
        logger.info("Truncating bronze tables")
        loader.truncate_bronze_tables()

    # Load prices data
    logger.info("Loading prices data to bronze layer")
    loader.load_to_bronze(
        data=prices_data, table_name="raw_prices", source_url=API_BASE_URL, api_endpoint="/prices/prices"
    )

    # Load food balance data
    logger.info("Loading food balance data to bronze layer")
    loader.load_to_bronze(
        data=food_balance_data,
        table_name="raw_food_balance",
        source_url=API_BASE_URL,
        api_endpoint="/food/food_balance_sheets",
    )

    logger.info("Bronze layer loading complete")


@task(name="Run dbt Models")
def run_dbt_transformations(target: str = "dev"):
    """Run dbt models to transform bronze → silver → gold."""
    logger = get_run_logger()
    logger.info(f"Starting dbt transformations (target={target})")

    # Change to dbt project directory
    os.chdir(DBT_PROJECT_PATH)

    # Note: dbt deps is not needed since we're using custom macros

    # Run all models
    logger.info("Running dbt models")
    result = trigger_dbt_cli_command(
        command=f"dbt run --target {target}", project_dir=str(DBT_PROJECT_PATH), profiles_dir=str(DBT_PROJECT_PATH)
    )

    logger.info("dbt transformations complete")
    return result


@task(name="Run dbt Tests")
def run_dbt_tests(target: str = "dev"):
    """Run dbt tests for data quality validation."""
    logger = get_run_logger()
    logger.info("Running dbt tests")

    os.chdir(DBT_PROJECT_PATH)

    result = trigger_dbt_cli_command(
        command=f"dbt test --target {target}", project_dir=str(DBT_PROJECT_PATH), profiles_dir=str(DBT_PROJECT_PATH)
    )

    logger.info("dbt tests complete")
    return result


@task(name="Generate dbt Documentation")
def generate_dbt_docs(target: str = "dev"):
    """Generate dbt documentation."""
    logger = get_run_logger()
    logger.info("Generating dbt documentation")

    os.chdir(DBT_PROJECT_PATH)

    result = trigger_dbt_cli_command(
        command=f"dbt docs generate --target {target}",
        project_dir=str(DBT_PROJECT_PATH),
        profiles_dir=str(DBT_PROJECT_PATH),
    )

    logger.info("dbt documentation generated")
    return result


@flow(
    name="FAO ELT Pipeline",
    description="Extract FAO data, Load to Bronze, Transform through Silver to Gold",
    task_runner=SequentialTaskRunner(),
)
def fao_elt_pipeline(
    max_pages_prices: int = 3,
    max_pages_food_balance: int = 3,
    truncate_bronze: bool = True,
    run_tests: bool = True,
    generate_docs: bool = False,
):
    """
    Main ELT pipeline flow.

    Args:
        max_pages_prices: Maximum pages to fetch from prices API
        max_pages_food_balance: Maximum pages to fetch from food balance API
        truncate_bronze: Whether to truncate bronze tables before loading
        run_tests: Whether to run dbt tests after transformations
        generate_docs: Whether to generate dbt documentation
    """
    logger = get_run_logger()
    logger.info("Starting FAO ELT Pipeline")

    # Extract phase
    prices_data = extract_prices_data(max_pages=max_pages_prices)
    food_balance_data = extract_food_balance_data(max_pages=max_pages_food_balance)

    # Load phase (to Bronze)
    load_to_bronze(prices_data=prices_data, food_balance_data=food_balance_data, truncate_first=truncate_bronze)

    # Transform phase (Bronze → Silver → Gold)
    dbt_result = run_dbt_transformations()

    # Optional: Run tests
    if run_tests:
        test_result = run_dbt_tests()

    # Optional: Generate documentation
    if generate_docs:
        result = generate_dbt_docs()
        logger.info(f"dbt documentation generated: {result}")

    logger.info("FAO ELT Pipeline completed successfully")


if __name__ == "__main__":
    # Run the pipeline with default settings
    fao_elt_pipeline(
        max_pages_prices=2,  # Fetch 2 pages for demo
        max_pages_food_balance=2,
        truncate_bronze=True,
        run_tests=True,
        generate_docs=True,
    )
