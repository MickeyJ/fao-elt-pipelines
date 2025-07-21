"""Database client for extracting data from remote PostgreSQL database."""

import logging
import os
import time

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PGClient:
    """
    Client for extracting data from the remote PostgreSQL database.
    Designed to handle large datasets efficiently with chunking and filtering.
    """

    def __init__(self):
        """Initialize database client with remote connection parameters."""
        self.conn_params = {
            "host": os.getenv("REMOTE_DB_HOST"),
            "port": os.getenv("REMOTE_DB_PORT", "5432"),
            "database": os.getenv("REMOTE_DB_NAME"),
            "user": os.getenv("REMOTE_DB_USER"),
            "password": os.getenv("REMOTE_DB_PASSWORD"),
        }
        self._validate_connection_params()

    def _validate_connection_params(self):
        """Validate that all required connection parameters are provided."""
        required_params = ["host", "database", "user", "password"]
        missing = [param for param in required_params if not self.conn_params.get(param)]

        if missing:
            raise ValueError(f"Missing required database connection parameters: {missing}")

    def test_connectivity(self) -> tuple[bool, str]:
        """
        Test database connectivity and return status.

        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            logger.info("Testing remote FAO database connectivity")
            conn = psycopg2.connect(**self.conn_params)
            cursor = conn.cursor()

            # Test query to get basic info
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]

            # Check if trade_detailed_trade_matrix exists and get row count
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'trade_detailed_trade_matrix'
            """)
            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM trade_detailed_trade_matrix LIMIT 1;")
                # For large tables, we'll use a sample count
                cursor.execute("""
                    SELECT reltuples::BIGINT AS estimate
                    FROM pg_class
                    WHERE relname = 'trade_detailed_trade_matrix'
                """)
                estimated_rows = cursor.fetchone()[0]

                cursor.close()
                conn.close()

                return (
                    True,
                    f"Connected successfully. PostgreSQL {version.split()[1]}, trade_detailed_trade_matrix has ~{estimated_rows:,} rows",
                )
            else:
                cursor.close()
                conn.close()
                return (
                    False,
                    "Connected to database but trade_detailed_trade_matrix table not found",
                )

        except psycopg2.OperationalError as e:
            return False, f"Database connection failed: {e!s}"
        except Exception as e:
            return False, f"Unexpected error during connectivity test: {e!s}"

    def get_table_info(self, table_name: str = "trade_detailed_trade_matrix") -> dict:
        """
        Get detailed information about the trade table structure and constraints.

        Args:
            table_name: Name of the table to analyze

        Returns:
            Dict containing table metadata
        """
        try:
            conn = psycopg2.connect(**self.conn_params)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get column information
            cursor.execute(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )
            columns = cursor.fetchall()

            # Get index information
            cursor.execute(
                """
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = %s
            """,
                (table_name,),
            )
            indexes = cursor.fetchall()

            # Get row count estimate
            cursor.execute(
                """
                SELECT reltuples::BIGINT AS estimate
                FROM pg_class
                WHERE relname = %s
            """,
                (table_name,),
            )
            row_estimate = cursor.fetchone()["estimate"] if cursor.rowcount > 0 else 0

            cursor.close()
            conn.close()

            return {
                "table_name": table_name,
                "columns": [dict(col) for col in columns],
                "indexes": [dict(idx) for idx in indexes],
                "estimated_rows": row_estimate,
                "analyzed_at": time.time(),
            }

        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            raise

    def extract_data_chunked(
        self,
        chunk_size: int = 50000,
        max_chunks: int = 50,
        query: str | None = None,
    ) -> tuple[list[dict], dict]:
        """
        Extract trade data in chunks with intelligent filtering for efficiency.

        Args:
            chunk_size: Number of rows per chunk
            max_chunks: Maximum number of chunks to extract
            query: SQL query to extract data

        Returns:
            Tuple[List[Dict], Dict]: (extracted_data, metadata)
        """
        metadata = {
            "chunk_size": chunk_size,
            "max_chunks": max_chunks,
            "chunks_processed": 0,
            "total_rows_extracted": 0,
            "filters_applied": {},
            "extraction_duration": 0,
            "errors": [],
            "warnings": [],
        }

        start_time = time.time()
        all_data = []

        try:
            # Build the filtering query

            conn = psycopg2.connect(**self.conn_params)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            logger.info(f"Starting chunked extraction with query: {query}")

            # Get total count for this filtered query
            count_query = f"SELECT COUNT(*) FROM ({query}) as filtered_data"
            cursor.execute(count_query)
            total_available = cursor.fetchone()["count"]

            logger.info(f"Total rows matching filters: {total_available:,}")

            # Extract data in chunks
            offset = 0
            chunk_num = 0

            while chunk_num < max_chunks:
                chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"

                logger.info(f"Extracting chunk {chunk_num + 1}/{max_chunks} (offset: {offset:,})")

                cursor.execute(chunk_query)
                chunk_data = cursor.fetchall()

                if not chunk_data:
                    logger.info("No more data available")
                    break

                # Convert to list of dicts
                chunk_list = [dict(row) for row in chunk_data]
                all_data.extend(chunk_list)

                metadata["chunks_processed"] += 1
                metadata["total_rows_extracted"] += len(chunk_list)

                logger.info(f"✅ Chunk {chunk_num + 1}: {len(chunk_list):,} rows extracted")

                # Break if we got less than chunk_size (last chunk)
                if len(chunk_data) < chunk_size:
                    logger.info("Reached end of data")
                    break

                offset += chunk_size
                chunk_num += 1

                # Small delay to be nice to the database
                time.sleep(0.1)

            cursor.close()
            conn.close()

            metadata["extraction_duration"] = round(time.time() - start_time, 2)

            logger.info(
                f"✅ Extraction complete: {len(all_data):,} total rows in {metadata['extraction_duration']}s"
            )

            return all_data, metadata

        except Exception as e:
            metadata["errors"].append(str(e))
            logger.error(f"Error during chunked extraction: {e}")
            raise
