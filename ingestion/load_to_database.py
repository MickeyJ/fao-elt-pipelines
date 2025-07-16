"""Load raw data from API to PostgreSQL bronze layer."""

import logging
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresLoader:
    """Handle loading data to PostgreSQL bronze layer."""

    def __init__(self):
        self.conn_params = {
            "host": os.getenv("LOCAL_DB_HOST", "localhost"),
            "port": os.getenv("LOCAL_DB_PORT", "5432"),
            "database": os.getenv("LOCAL_DB_NAME", "fao"),
            "user": os.getenv("LOCAL_DB_USER", "mickey"),
            "password": os.getenv("LOCAL_DB_PASSWORD"),
        }

    def get_connection(self):
        """Get PostgreSQL connection."""
        return psycopg2.connect(**self.conn_params)

    def load_to_bronze(self, data: list[dict], table_name: str, source_url: str, api_endpoint: str):
        """
        Load raw JSON data to bronze layer table.

        Args:
            data: List of dictionaries to load
            table_name: Target table name in bronze schema
            source_url: Source API URL
            api_endpoint: Specific endpoint used
        """
        if not data:
            logger.warning(f"No data to load for {table_name}")
            return

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Insert each record as JSONB
                insert_query = f"""
                    INSERT INTO bronze.{table_name} (data, source_url, api_endpoint)
                    VALUES (%s, %s, %s)
                """

                # Batch insert for better performance
                records = [(Json(record), source_url, api_endpoint) for record in data]

                cur.executemany(insert_query, records)
                conn.commit()

                logger.info(f"Loaded {len(records)} records to bronze.{table_name}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading data to {table_name}: {e}")
            raise
        finally:
            conn.close()

    def truncate_bronze_tables(self):
        """Truncate bronze tables before fresh load (for demo purposes)."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE bronze.raw_prices, bronze.raw_food_balance CASCADE")
                conn.commit()
                logger.info("Truncated bronze tables")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error truncating tables: {e}")
            raise
        finally:
            conn.close()
