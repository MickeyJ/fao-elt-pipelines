"""File client for processing CSV and Excel files as data sources."""

import logging
import time
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileClient:
    """
    Client for processing CSV and Excel files as data sources.
    Converts files to standardized JSON format for bronze layer storage.
    """

    def __init__(self, upload_directory: str = "uploads"):
        """Initialize file client with upload directory."""
        self.upload_directory = Path(upload_directory)
        self.upload_directory.mkdir(exist_ok=True)

        # Supported file extensions
        self.supported_extensions = {".csv", ".xlsx", ".xls", ".tsv", ".txt"}

    def validate_file(self, file_path: Path) -> tuple[bool, str]:
        """
        Validate if file exists and is supported format.

        Args:
            file_path: Path to the file to validate

        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        if not file_path.exists():
            return False, f"File does not exist: {file_path}"

        if file_path.suffix.lower() not in self.supported_extensions:
            return (
                False,
                f"Unsupported file type: {file_path.suffix}. Supported: {self.supported_extensions}",
            )

        try:
            file_size = file_path.stat().st_size
            if file_size == 0:
                return False, "File is empty"
            elif file_size > 500 * 1024 * 1024:  # 500MB limit
                return False, f"File too large: {file_size / 1024 / 1024:.1f}MB (max 500MB)"

            return True, f"File valid: {file_size / 1024 / 1024:.2f}MB"

        except Exception as e:
            return False, f"Error validating file: {e}"

    def detect_file_structure(self, file_path: Path) -> dict:
        """
        Analyze file structure and detect optimal parsing parameters.

        Args:
            file_path: Path to the file to analyze

        Returns:
            Dict with file structure information
        """
        try:
            file_extension = file_path.suffix.lower()

            # Basic file info
            file_stats = file_path.stat()
            structure_info = {
                "filename": file_path.name,
                "file_extension": file_extension,
                "file_size_mb": round(file_stats.st_size / 1024 / 1024, 2),
                "modified_time": file_stats.st_mtime,
                "detected_delimiter": None,
                "detected_encoding": "utf-8",
                "estimated_rows": 0,
                "column_count": 0,
                "column_names": [],
                "sample_data": [],
                "parsing_suggestions": {},
            }

            # Read sample for analysis
            if file_extension in [".csv", ".tsv", ".txt"]:
                # Detect delimiter and encoding
                with open(file_path, "rb") as f:
                    sample_bytes = f.read(10000)

                # Try to detect encoding
                try:
                    sample_text = sample_bytes.decode("utf-8")
                    structure_info["detected_encoding"] = "utf-8"
                except UnicodeDecodeError:
                    try:
                        sample_text = sample_bytes.decode("latin-1")
                        structure_info["detected_encoding"] = "latin-1"
                    except Exception:
                        structure_info["detected_encoding"] = "utf-8"  # fallback
                        sample_text = sample_bytes.decode("utf-8", errors="ignore")

                # Detect delimiter
                delimiters = [",", "\t", ";", "|"]
                delimiter_counts = {d: sample_text.count(d) for d in delimiters}
                detected_delimiter = max(delimiter_counts.items(), key=lambda x: x[1])[0]
                structure_info["detected_delimiter"] = detected_delimiter

                # Read sample with pandas
                df_sample = pd.read_csv(
                    file_path,
                    delimiter=detected_delimiter,
                    encoding=structure_info["detected_encoding"],
                    nrows=100,
                    low_memory=False,
                )

            elif file_extension in [".xlsx", ".xls"]:
                # Read Excel file
                df_sample = pd.read_excel(file_path, nrows=100)
                structure_info["parsing_suggestions"]["sheet_names"] = pd.ExcelFile(
                    file_path
                ).sheet_names

            else:
                raise ValueError(f"Unsupported file type: {file_extension}")

            # Analyze the sample DataFrame
            structure_info.update(
                {
                    "column_count": len(df_sample.columns),
                    "column_names": df_sample.columns.tolist(),
                    "sample_data": df_sample.head(5).to_dict("records"),
                    "estimated_rows": self._estimate_total_rows(file_path, len(df_sample)),
                }
            )

            # Generate parsing suggestions
            structure_info["parsing_suggestions"].update(
                {
                    "recommended_chunk_size": min(
                        10000, max(1000, structure_info["estimated_rows"] // 10)
                    ),
                    "has_header": True,  # Assume header row exists
                    "numeric_columns": df_sample.select_dtypes(include=["number"]).columns.tolist(),
                    "text_columns": df_sample.select_dtypes(include=["object"]).columns.tolist(),
                    "date_columns": df_sample.select_dtypes(include=["datetime"]).columns.tolist(),
                }
            )

            return structure_info

        except Exception as e:
            logger.error(f"Error detecting file structure for {file_path}: {e}")
            return {
                "filename": file_path.name,
                "error": str(e),
                "parsing_suggestions": {"recommended_chunk_size": 1000},
            }

    def _estimate_total_rows(self, file_path: Path, sample_rows: int) -> int:
        """Estimate total rows in file based on file size and sample."""
        try:
            if file_path.suffix.lower() in [".xlsx", ".xls"]:
                # For Excel, count rows directly (small overhead)
                df = pd.read_excel(file_path, usecols=[0])  # Read just first column
                return len(df)
            else:
                # For CSV, estimate based on file size
                file_size = file_path.stat().st_size
                with open(file_path, "rb") as f:
                    first_100_lines = sum(
                        1 for _ in f.read(10000).decode("utf-8", errors="ignore").splitlines()
                    )
                    f.seek(0)
                    first_100_bytes = len(f.read(10000))

                if first_100_bytes > 0:
                    estimated_total = int((file_size / first_100_bytes) * first_100_lines)
                    return max(estimated_total, sample_rows)
                return sample_rows

        except Exception:
            return sample_rows

    def process_file_to_records(
        self, file_path: Path, chunk_size: int = 5000, max_rows: int | None = None
    ) -> tuple[list[dict], dict]:
        """
        Process file into records suitable for bronze layer storage.

        Args:
            file_path: Path to the file to process
            chunk_size: Number of rows to process at once
            max_rows: Maximum number of rows to process (None = all)

        Returns:
            Tuple[List[Dict], Dict]: (records, metadata)
        """
        start_time = time.time()

        # Validate file first
        is_valid, validation_message = self.validate_file(file_path)
        if not is_valid:
            raise ValueError(f"File validation failed: {validation_message}")

        # Get file structure
        structure_info = self.detect_file_structure(file_path)

        metadata = {
            "filename": file_path.name,
            "file_path": str(file_path),
            "file_structure": structure_info,
            "processing_params": {"chunk_size": chunk_size, "max_rows": max_rows},
            "total_records_processed": 0,
            "chunks_processed": 0,
            "processing_duration": 0,
            "errors": [],
            "warnings": [],
        }

        try:
            all_records = []
            chunk_count = 0
            rows_processed = 0

            file_extension = file_path.suffix.lower()

            # Process file in chunks
            if file_extension in [".csv", ".tsv", ".txt"]:
                delimiter = structure_info.get("detected_delimiter", ",")
                encoding = structure_info.get("detected_encoding", "utf-8")

                # Use pandas read_csv with chunking
                chunk_reader = pd.read_csv(
                    file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    chunksize=chunk_size,
                    low_memory=False,
                    nrows=max_rows,
                )

                for chunk_df in chunk_reader:
                    if max_rows and rows_processed >= max_rows:
                        break

                    # Convert chunk to records
                    chunk_records = self._dataframe_to_records(chunk_df, chunk_count)
                    all_records.extend(chunk_records)

                    chunk_count += 1
                    rows_processed += len(chunk_df)

                    logger.info(f"Processed chunk {chunk_count}: {len(chunk_df)} rows")

            elif file_extension in [".xlsx", ".xls"]:
                # For Excel, read in chunks manually
                total_rows = structure_info.get("estimated_rows", 0)

                for start_row in range(0, total_rows, chunk_size):
                    if max_rows and start_row >= max_rows:
                        break

                    end_row = min(start_row + chunk_size, total_rows)
                    if max_rows:
                        end_row = min(end_row, max_rows)

                    chunk_df = pd.read_excel(
                        file_path, skiprows=start_row if start_row > 0 else None, nrows=chunk_size
                    )

                    if chunk_df.empty:
                        break

                    # Convert chunk to records
                    chunk_records = self._dataframe_to_records(chunk_df, chunk_count)
                    all_records.extend(chunk_records)

                    chunk_count += 1
                    rows_processed += len(chunk_df)

                    logger.info(f"Processed Excel chunk {chunk_count}: {len(chunk_df)} rows")

            # Finalize metadata
            metadata.update(
                {
                    "total_records_processed": len(all_records),
                    "chunks_processed": chunk_count,
                    "processing_duration": round(time.time() - start_time, 2),
                }
            )

            logger.info(
                f"✅ File processing complete: {len(all_records):,} records in {metadata['processing_duration']}s"
            )

            return all_records, metadata

        except Exception as e:
            metadata["errors"].append(str(e))
            logger.error(f"Error processing file {file_path}: {e}")
            raise

    def _dataframe_to_records(self, df: pd.DataFrame, chunk_number: int) -> list[dict]:
        """Convert DataFrame chunk to list of dictionaries with metadata."""
        records = []

        for index, row in df.iterrows():
            # Convert row to dict, handling NaN values
            record = {}
            for col, value in row.items():
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (pd.Timestamp, pd.datetime)):
                    record[col] = value.isoformat()
                else:
                    record[col] = value

            # Add metadata to each record
            record["_file_metadata"] = {
                "chunk_number": chunk_number,
                "row_number": index,
                "processed_at": time.time(),
            }

            records.append(record)

        return records

    def get_upload_suggestions(self, file_path: Path) -> dict:
        """
        Analyze file and provide upload/processing suggestions.

        Args:
            file_path: Path to the file to analyze

        Returns:
            Dict with processing suggestions
        """
        structure_info = self.detect_file_structure(file_path)

        suggestions = {
            "recommended_chunk_size": structure_info.get("parsing_suggestions", {}).get(
                "recommended_chunk_size", 5000
            ),
            "estimated_processing_time": "< 1 minute",
            "memory_requirements": "Low",
            "recommended_max_rows": None,  # Process all by default
            "data_quality_notes": [],
            "processing_strategy": "standard",
        }

        # Adjust based on file size
        file_size_mb = structure_info.get("file_size_mb", 0)
        estimated_rows = structure_info.get("estimated_rows", 0)

        if file_size_mb > 100:
            suggestions.update(
                {
                    "recommended_chunk_size": 2000,
                    "estimated_processing_time": "2-5 minutes",
                    "memory_requirements": "Medium",
                    "processing_strategy": "chunked",
                }
            )
            suggestions["data_quality_notes"].append("Large file - will process in smaller chunks")

        if estimated_rows > 100000:
            suggestions["data_quality_notes"].append("Consider limiting rows for initial testing")
            suggestions["recommended_max_rows"] = 50000

        # Check for potential data quality issues
        if structure_info.get("column_count", 0) > 50:
            suggestions["data_quality_notes"].append(
                "Many columns detected - consider selecting key columns"
            )

        return suggestions

    def list_uploaded_files(self) -> list[dict]:
        """List all uploaded files with basic metadata."""
        files = []

        for file_path in self.upload_directory.glob("*"):
            if file_path.is_file() and file_path.suffix.lower() in self.supported_extensions:
                try:
                    stats = file_path.stat()
                    files.append(
                        {
                            "filename": file_path.name,
                            "path": str(file_path),
                            "size_mb": round(stats.st_size / 1024 / 1024, 2),
                            "modified": stats.st_mtime,
                            "extension": file_path.suffix.lower(),
                            "is_supported": True,
                        }
                    )
                except Exception as e:
                    files.append(
                        {
                            "filename": file_path.name,
                            "path": str(file_path),
                            "error": str(e),
                            "is_supported": False,
                        }
                    )

        return sorted(files, key=lambda x: x.get("modified", 0), reverse=True)
