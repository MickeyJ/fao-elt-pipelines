"""
Corrected FAO ELT Pipeline - One request per endpoint with original query params + limit=500
"""

import os
import subprocess
import sys
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from psycopg2.extras import Json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

load_dotenv(override=True)

# Configuration
API_BASE_URL = os.getenv("FAO_API_BASE_URL", "https://kw2aqt7p3p.us-west-2.awsapprunner.com/v1")
PRICES_ENDPOINT = os.getenv(
    "PRICES_ENDPOINT", "/prices/prices/?element_code=5532&element=&flag=A&sort=year%2Citem_code"
)
FOOD_BALANCE_ENDPOINT = os.getenv(
    "FOOD_BALANCE_ENDPOINT",
    "/food/food_balance_sheets/?element_code=5511&flag=&sort=year%2Citem_code",
)
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt_project"
ALLOWED_DBT_TARGETS = ["dev", "prod", "test"]


class CorrectedFAOApiClient:
    """FAO API Client with correct query parameters."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()

        # Add retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def test_connectivity(self) -> tuple[bool, str]:
        """Test if the API is accessible."""
        try:
            test_url = f"{self.base_url}/prices/prices/?limit=1"
            response = self.session.get(test_url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                return True, f"API accessible. Test returned {len(data['data'])} records."
            elif isinstance(data, list) and len(data) > 0:
                return True, f"API accessible. Test returned {len(data)} records."
            else:
                return True, "API accessible but no data in test response."
        except requests.exceptions.RequestException as e:
            return False, f"API connectivity test failed: {e!s}"
        except Exception as e:
            return False, f"Unexpected error during connectivity test: {e!s}"

    def fetch_data(self, endpoint: str, limit: int = 500) -> tuple[list[dict], dict]:
        """
        Fetch data from a single endpoint with limit parameter.

        Args:
            endpoint: API endpoint path (with existing query params)
            limit: Number of records to fetch

        Returns:
            Tuple[List[Dict], Dict]: (data_records, metadata)
        """
        metadata = {
            "endpoint": endpoint,
            "limit": limit,
            "total_records": 0,
            "errors": [],
            "warnings": [],
            "sample_record": None,
        }

        # Build full URL
        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        # Add limit parameter to existing query params
        if "?" in url:
            url += f"&limit={limit}"
        else:
            url += f"?limit={limit}"

        try:
            print(f"🔄 Fetching data from: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            result = response.json()
            print(
                f"📋 Response keys: {list(result.keys()) if isinstance(result, dict) else 'List response'}"
            )

            # Extract data from response
            data = []
            if isinstance(result, dict) and "data" in result:
                data = result["data"]
            elif isinstance(result, dict) and "results" in result:
                data = result["results"]
            elif isinstance(result, list):
                data = result
            else:
                warning_msg = f"Unexpected response structure. Keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}"
                metadata["warnings"].append(warning_msg)
                print(f"⚠️  {warning_msg}")

            metadata["total_records"] = len(data)

            if data:
                metadata["sample_record"] = data[0]
                print(f"✅ Fetched {len(data)} records")
                print(
                    f"📄 Sample record keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dict'}"
                )
            else:
                metadata["warnings"].append("No data found in response")
                print("⚠️  No data found in response")

            return data, metadata

        except requests.exceptions.RequestException as e:
            error_msg = f"Request error: {e!s}"
            metadata["errors"].append(error_msg)
            print(f"❌ {error_msg}")
            return [], metadata
        except Exception as e:
            error_msg = f"Unexpected error: {e!s}"
            metadata["errors"].append(error_msg)
            print(f"❌ {error_msg}")
            return [], metadata

    def fetch_prices_data(self, limit: int = 500) -> tuple[list[dict], dict]:
        """Fetch producer prices data with EXACT original query parameters."""
        endpoint = "/prices/prices/?element_code=5532&element=&flag=A&sort=year%2Citem_code"
        return self.fetch_data(endpoint, limit)

    def fetch_food_balance_data(self, limit: int = 500) -> tuple[list[dict], dict]:
        """Fetch food balance sheets data with EXACT original query parameters."""
        endpoint = "/food/food_balance_sheets/?element_code=5511&flag=&sort=year%2Citem_code"
        return self.fetch_data(endpoint, limit)


class CorrectedPostgresLoader:
    """PostgreSQL loader with validation."""

    def __init__(self):
        self.conn_params = {
            "host": os.getenv("LOCAL_DB_HOST", "localhost"),
            "port": int(os.getenv("LOCAL_DB_PORT", "5432")),
            "database": os.getenv("LOCAL_DB_NAME", "fao"),
            "user": os.getenv("LOCAL_DB_USER", "mickey"),
            "password": os.getenv("LOCAL_DB_PASSWORD"),
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test database connectivity and schema existence."""
        try:
            conn = psycopg2.connect(**self.conn_params)
            with conn.cursor() as cur:
                # Test connection
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]

                # Check if schemas exist
                cur.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name IN ('bronze', 'silver', 'gold')
                """)
                schemas = [row[0] for row in cur.fetchall()]

                # Check if bronze tables exist
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'bronze'
                    AND table_name IN ('raw_prices', 'raw_food_balance')
                """)
                tables = [row[0] for row in cur.fetchall()]

            conn.close()

            return True, f"Connected to PostgreSQL. Schemas: {schemas}, Bronze tables: {tables}"

        except Exception as e:
            return False, f"Database connection failed: {e!s}"

    def get_connection(self):
        """Get PostgreSQL connection with error handling."""
        try:
            return psycopg2.connect(**self.conn_params)
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e!s}")

    def load_to_bronze(
        self, data: list[dict], metadata: dict, table_name: str, source_url: str, api_endpoint: str
    ) -> dict:
        """
        Load raw JSON data to bronze layer with validation.

        Returns:
            Dict: Loading results and statistics
        """
        result = {
            "table_name": table_name,
            "records_attempted": len(data),
            "records_loaded": 0,
            "errors": [],
            "warnings": [],
            "metadata": metadata,
        }

        if not data:
            error_msg = f"CRITICAL: No data provided for {table_name}. API metadata: {metadata}"
            result["errors"].append(error_msg)
            raise ValueError(error_msg)

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Validate table exists
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'bronze' AND table_name = %s
                """,
                    (table_name,),
                )

                if cur.fetchone()[0] == 0:
                    error_msg = f"Bronze table '{table_name}' does not exist"
                    result["errors"].append(error_msg)
                    raise Exception(error_msg)

                # Prepare data with validation
                valid_records = []
                for i, record in enumerate(data):
                    try:
                        # Validate record is a dict
                        if not isinstance(record, dict):
                            result["warnings"].append(f"Record {i} is not a dict: {type(record)}")
                            continue

                        # Create the insert record
                        insert_record = (Json(record), source_url, api_endpoint)
                        valid_records.append(insert_record)

                    except Exception as e:
                        result["warnings"].append(f"Record {i} validation failed: {e!s}")

                if not valid_records:
                    error_msg = f"No valid records found for {table_name}"
                    result["errors"].append(error_msg)
                    raise Exception(error_msg)

                # Batch insert
                insert_query = f"""
                    INSERT INTO bronze.{table_name} (data, source_url, api_endpoint, loaded_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                """

                cur.executemany(insert_query, valid_records)
                conn.commit()

                result["records_loaded"] = len(valid_records)

                # Verify insertion
                cur.execute(f"SELECT COUNT(*) FROM bronze.{table_name}")
                total_count = cur.fetchone()[0]

                print(f"✅ Loaded {result['records_loaded']} records to bronze.{table_name}")
                print(f"📊 Total records in bronze.{table_name}: {total_count}")

                # Store sample for debugging
                if result["records_loaded"] > 0:
                    cur.execute(
                        f"SELECT data FROM bronze.{table_name} ORDER BY loaded_at DESC LIMIT 1"
                    )
                    sample = cur.fetchone()[0]
                    result["sample_loaded_record"] = sample

        except Exception as e:
            conn.rollback()
            error_msg = f"Error loading data to {table_name}: {e!s}"
            result["errors"].append(error_msg)
            raise Exception(error_msg)
        finally:
            conn.close()

        return result

    def truncate_bronze_tables(self) -> dict:
        """Truncate bronze tables with validation."""
        result = {"truncated_tables": [], "errors": []}

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Check which tables exist before truncating
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'bronze'
                    AND table_name IN ('raw_prices', 'raw_food_balance')
                """)
                existing_tables = [row[0] for row in cur.fetchall()]

                if not existing_tables:
                    result["errors"].append("No bronze tables found to truncate")
                    return result

                for table in existing_tables:
                    cur.execute(f"TRUNCATE TABLE bronze.{table} CASCADE")
                    result["truncated_tables"].append(table)

                conn.commit()
                print(f"🗑️  Truncated bronze tables: {result['truncated_tables']}")

        except Exception as e:
            conn.rollback()
            error_msg = f"Error truncating tables: {e!s}"
            result["errors"].append(error_msg)
            raise Exception(error_msg)
        finally:
            conn.close()

        return result


def validate_dbt_target(target: str) -> None:
    """Validate dbt target to prevent command injection."""
    if target not in ALLOWED_DBT_TARGETS:
        raise ValueError(f"Invalid target '{target}'. Must be one of: {ALLOWED_DBT_TARGETS}")


@task(name="Test System Connectivity", retries=1)
def test_system_connectivity():
    """Test API and database connectivity before starting pipeline."""
    logger = get_run_logger()
    logger.info("🔍 Testing system connectivity...")

    # Test API
    client = CorrectedFAOApiClient(API_BASE_URL)
    api_ok, api_msg = client.test_connectivity()
    logger.info(f"API Test: {api_msg}")

    # Test Database
    loader = CorrectedPostgresLoader()
    db_ok, db_msg = loader.test_connection()
    logger.info(f"Database Test: {db_msg}")

    if not api_ok:
        raise Exception(f"API connectivity failed: {api_msg}")

    if not db_ok:
        raise Exception(f"Database connectivity failed: {db_msg}")

    logger.info("✅ All systems are connected and ready")
    return {"api_status": api_msg, "db_status": db_msg}


@task(name="Extract Prices Data", retries=2, retry_delay_seconds=30)
def extract_prices_data(limit: int = 500) -> tuple[list[dict], dict]:
    """Extract prices data - single request with limit."""
    logger = get_run_logger()
    logger.info(f"🔄 Starting prices data extraction (limit={limit})")

    client = CorrectedFAOApiClient(API_BASE_URL)
    data, metadata = client.fetch_prices_data(limit=limit)

    logger.info(f"📊 Extracted {len(data)} price records")

    if metadata.get("errors"):
        logger.warning(f"⚠️  Extraction errors: {metadata['errors']}")

    if not data:
        logger.error("❌ CRITICAL: No price data extracted!")
        raise ValueError(f"No price data extracted. Metadata: {metadata}")

    return data, metadata


@task(name="Extract Food Balance Data", retries=2, retry_delay_seconds=30)
def extract_food_balance_data(limit: int = 500) -> tuple[list[dict], dict]:
    """Extract food balance data - single request with limit."""
    logger = get_run_logger()
    logger.info(f"🔄 Starting food balance data extraction (limit={limit})")

    client = CorrectedFAOApiClient(API_BASE_URL)
    data, metadata = client.fetch_food_balance_data(limit=limit)

    logger.info(f"📊 Extracted {len(data)} food balance records")

    if metadata.get("errors"):
        logger.warning(f"⚠️  Extraction errors: {metadata['errors']}")

    if not data:
        logger.error("❌ CRITICAL: No food balance data extracted!")
        raise ValueError(f"No food balance data extracted. Metadata: {metadata}")

    return data, metadata


@task(name="Load to Bronze Layer")
def load_to_bronze(
    prices_data_and_metadata: tuple[list[dict], dict],
    food_balance_data_and_metadata: tuple[list[dict], dict],
    truncate_first: bool = True,
) -> dict:
    """Load raw data to PostgreSQL bronze layer with validation."""
    logger = get_run_logger()
    loader = CorrectedPostgresLoader()

    prices_data, prices_metadata = prices_data_and_metadata
    food_balance_data, food_balance_metadata = food_balance_data_and_metadata

    results = {"truncation": None, "prices": None, "food_balance": None}

    if truncate_first:
        logger.info("🗑️  Truncating bronze tables")
        results["truncation"] = loader.truncate_bronze_tables()

    # Load prices data
    logger.info("📥 Loading prices data to bronze layer")
    results["prices"] = loader.load_to_bronze(
        data=prices_data,
        metadata=prices_metadata,
        table_name="raw_prices",
        source_url=API_BASE_URL,
        api_endpoint=prices_metadata.get("endpoint", "unknown"),
    )

    # Load food balance data
    logger.info("📥 Loading food balance data to bronze layer")
    results["food_balance"] = loader.load_to_bronze(
        data=food_balance_data,
        metadata=food_balance_metadata,
        table_name="raw_food_balance",
        source_url=API_BASE_URL,
        api_endpoint=food_balance_metadata.get("endpoint", "unknown"),
    )

    logger.info("✅ Bronze layer loading complete")
    logger.info(f"📊 Prices: {results['prices']['records_loaded']} records")
    logger.info(f"📊 Food Balance: {results['food_balance']['records_loaded']} records")

    return results


@task(name="Run dbt Models")
def run_dbt_transformations(target: str = "dev") -> dict:
    """Run dbt models with enhanced error handling."""
    logger = get_run_logger()
    validate_dbt_target(target)

    logger.info(f"🔄 Starting dbt transformations (target={target})")

    result = subprocess.run(
        ["dbt", "run", "--target", target],
        cwd=str(DBT_PROJECT_PATH),
        capture_output=True,
        text=True,
    )

    # Parse results
    output_info = {
        "return_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "success": result.returncode == 0,
    }

    # Log detailed output
    if result.stdout:
        logger.info(f"📋 dbt output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"❌ dbt run failed:\n{result.stderr}")
        raise Exception(f"dbt run failed with return code {result.returncode}")

    logger.info("✅ dbt transformations complete")
    return output_info


@task(name="Run dbt Tests")
def run_dbt_tests(target: str = "dev") -> dict:
    """Run dbt tests with enhanced error handling."""
    logger = get_run_logger()
    validate_dbt_target(target)

    logger.info("🧪 Running dbt tests")

    result = subprocess.run(
        ["dbt", "test", "--target", target],
        cwd=str(DBT_PROJECT_PATH),
        capture_output=True,
        text=True,
    )

    # Parse results
    output_info = {
        "return_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "success": result.returncode == 0,
    }

    # Log detailed output
    if result.stdout:
        logger.info(f"📋 dbt test output:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"❌ dbt tests failed:\n{result.stderr}")
        # Don't raise exception for test failures, just log them
        logger.warning("Some dbt tests failed, but continuing pipeline")

    logger.info("✅ dbt tests complete")
    return output_info


@flow(
    name="Corrected FAO ELT Pipeline",
    description="ELT: One request per endpoint with CORRECT original query params + limit=500",
)
def corrected_fao_elt_pipeline(
    limit_prices: int = 500,
    limit_food_balance: int = 500,
    truncate_bronze: bool = True,
    run_tests: bool = True,
):
    """
    Main ELT pipeline flow with CORRECT query parameters.

    Args:
        limit_prices: Number of price records to fetch (single request)
        limit_food_balance: Number of food balance records to fetch (single request)
        truncate_bronze: Whether to truncate bronze tables before loading
        run_tests: Whether to run dbt tests after transformations
    """
    logger = get_run_logger()
    logger.info("🚀 Starting Corrected FAO ELT Pipeline")

    try:
        # Step 1: Test connectivity
        connectivity_status = test_system_connectivity()

        # Step 2: Extract phase (2 requests total)
        logger.info("📡 EXTRACT PHASE")
        prices_data_and_metadata = extract_prices_data(limit=limit_prices)
        food_balance_data_and_metadata = extract_food_balance_data(limit=limit_food_balance)

        # Step 3: Load phase (to Bronze)
        logger.info("📥 LOAD PHASE")
        load_results = load_to_bronze(
            prices_data_and_metadata=prices_data_and_metadata,
            food_balance_data_and_metadata=food_balance_data_and_metadata,
            truncate_first=truncate_bronze,
        )

        # Step 4: Transform phase (Bronze → Silver → Gold)
        logger.info("🔄 TRANSFORM PHASE")
        dbt_result = run_dbt_transformations()

        # Step 5: Optional tests
        test_result = None
        if run_tests:
            logger.info("🧪 TEST PHASE")
            test_result = run_dbt_tests()

        # Final success summary
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("📊 Final Summary:")
        logger.info(f"   - Prices loaded: {load_results['prices']['records_loaded']} records")
        logger.info(
            f"   - Food balance loaded: {load_results['food_balance']['records_loaded']} records"
        )
        logger.info(
            f"   - dbt transformations: {'✅ Success' if dbt_result['success'] else '❌ Failed'}"
        )

        return {
            "status": "success",
            "connectivity": connectivity_status,
            "load_results": load_results,
            "dbt_result": dbt_result,
            "test_result": test_result,
        }

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED: {e!s}")
        logger.error("🔍 Check the logs above for detailed error information")
        raise


if __name__ == "__main__":
    # Run the corrected pipeline
    corrected_fao_elt_pipeline(
        limit_prices=500,  # Single request for 500 price records
        limit_food_balance=500,  # Single request for 500 food balance records
        truncate_bronze=True,
        run_tests=True,
    )
