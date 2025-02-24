from datetime import datetime
import json
from typing import Dict, Any, List, Optional, Tuple, NamedTuple
import asyncpg
from logging import getLogger
from pydantic import BaseModel
from pathlib import Path
import hashlib
from dataclasses import dataclass

logger = getLogger(__name__)


class MetadataKey(NamedTuple):
    """
    Composite key for metadata lookup.
    """

    timestamp: datetime
    data_type: str
    parameter: str


@dataclass
class MetadataEntry:
    """Cached metadata entry."""

    file_path: str
    file_hash: str
    load_timestamp: datetime


@dataclass
class WeatherDataFile:
    """Container for weather data file information."""

    timestamp: datetime
    data_type: str
    parameter: str
    model_data: BaseModel
    file_path: Path
    file_hash: str


class BatchResult:
    """Results of a batch processing operation"""

    def __init__(self):
        self.processed: List[Path] = []
        self.skipped: List[Path] = []
        self.failed: List[Tuple[Path, str]] = []  # (path, error_message)


class WeatherStorage:
    """Simplified storage interface for weather data."""

    def __init__(self, dsn: str, batch_size: int = 100):
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None
        self.batch_size = batch_size
        self._metadata_cache: Dict[MetadataKey, MetadataEntry] = {}
        self._cache_initialized = False

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,  # Min no connections
                max_size=2,  # Max no connections
                max_queries=50,  # max queries per connection before recycling
                max_inactive_connection_lifetime=300.0,  # 5 miniutes
                timeout=60.0,  # Connection timesout
            )
        await self._initialize_metadata_cache()
        logger.info("Connected to database with optimized pool settings")

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._metadata_cache.clear()
            self._cache_initialized = False
        logger.info("Disconnected from database")

    async def clear_tables(self) -> None:
        """Clear all data from weather tables while preserving structure."""
        if not self._pool:
            raise RuntimeError("Not connected")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                logger.info("Starting table cleanup...")

                # Clear weather data
                await conn.execute("TRUNCATE TABLE raw_weather_data;")
                logger.info("Cleared raw_weather_data table")

                # Clear metadata
                await conn.execute("TRUNCATE TABLE weather_data_metadata;")
                logger.info("Cleared weather_data_metadata table")

                # Get current counts to verify
                raw_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM raw_weather_data;"
                )
                meta_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM weather_data_metadata;"
                )

                logger.info(
                    f"Verification - raw_weather_data: {raw_count} rows, weather_data_metadata: {meta_count} rows"
                )

    async def initialize_tables(self) -> None:
        if not self._pool:
            raise RuntimeError("Not connected")

        async with self._pool.acquire() as conn:
            logger.info("Initializing database tables...")

            # Create extensions
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            logger.info("TimescaleDB extension enabled")

            # Single raw data table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_weather_data (
                    data_timestamp TIMESTAMPTZ NOT NULL,
                    data_type TEXT NOT NULL,  -- 'reading' or 'forecast'
                    parameter TEXT NOT NULL,  -- 'temperature', 'pm25', etc.
                    validated_data JSONB NOT NULL,  -- Pydantic-validated data
                    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (data_timestamp, data_type, parameter)
                )
            """)
            logger.info("Raw weather data table created/verified")

            # Make it a hypertable
            await conn.execute(
                "SELECT create_hypertable('raw_weather_data', 'data_timestamp', if_not_exists => TRUE)"
            )
            logger.info("Hypertable configuration verified")

            # Create metadata tracking table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS weather_data_metadata (
                    data_timestamp TIMESTAMPTZ NOT NULL,
                    data_type TEXT NOT NULL,
                    parameter TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    load_timestamp TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (data_timestamp, data_type, parameter)
                );
            """)
            logger.info("Metadata tracking table created/verified")

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file content."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    async def check_file_processed(
        self, data_timestamp: datetime, data_type: str, parameter: str, file_path: Path
    ) -> Tuple[bool, Optional[str]]:
        """Check if file has been processed and hasn't changed using cached metadata."""
        if not self._cache_initialized:
            await self._initialize_metadata_cache()

        key = MetadataKey(data_timestamp, data_type, parameter)
        current_hash = self._calculate_file_hash(file_path)

        if key in self._metadata_cache:
            cached_entry = self._metadata_cache[key]
            if cached_entry.file_path == current_hash:
                return True, current_hash

        return False, current_hash

    async def store_data(
        self, data_type: str, parameter: str, model_data: BaseModel, file_path: Path
    ) -> None:
        """Store any validated weather data."""
        if not self._pool:
            raise RuntimeError("Not connected")

        timestamp = self._extract_timestamp(file_path)
        logger.info(f"\nProcessing file: {file_path}")
        logger.info(
            f"Parameters: type={data_type}, parameter={parameter}, timestamp={timestamp}"
        )

        # Check if the file has already been processed
        already_processed, file_hash = await self.check_file_processed(
            timestamp, data_type, parameter, file_path
        )

        if already_processed:
            logger.info(f"File already processed and unchanged: {file_path}")
            return

        async with self._pool.acquire() as conn:
            logger.info("Starting database transaction")
            # Start transaction
            async with conn.transaction():
                # Intesrt/update the weather data
                await conn.execute(
                    """
                    INSERT INTO raw_weather_data (
                        data_timestamp,
                        data_type,
                        parameter,
                        validated_data
                    )
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (data_timestamp, data_type, parameter) 
                    DO UPDATE SET 
                        validated_data = EXCLUDED.validated_data,
                        ingestion_timestamp = NOW()
                """,
                    timestamp,
                    data_type,
                    parameter,
                    json.dumps(model_data.model_dump(mode="json")),
                )
                logger.info("Weather data updated")

                # Update metadata
                await conn.execute(
                    """
                    INSERT INTO weather_data_metadata (
                        data_timestamp,
                        data_type,
                        parameter,
                        file_path,
                        file_hash
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (data_timestamp, data_type, parameter) 
                    DO UPDATE SET 
                        file_path = EXCLUDED.file_path,
                        file_hash = EXCLUDED.file_hash,
                        load_timestamp = NOW()
                """,
                    timestamp,
                    data_type,
                    parameter,
                    str(file_path),
                    file_hash,
                )
                logger.info("Metadata updated")
                logger.info("Transaction completed successfully")

    def _extract_timestamp(self, filename: Path) -> datetime:
        """Extract the primary timestamp from filename (YYYYMMDD_parameter.json)"""
        # Get date from filename
        date_str = filename.stem.split("_")[0]  # Gets YYYYMMDD
        base_date = datetime.strptime(date_str, "%Y%m%d")
        return base_date.replace(hour=0, minute=0, second=0, microsecond=0)

    async def prepare_file(
        self, data_type: str, parameter: str, model_data: BaseModel, file_path: Path
    ) -> Optional[WeatherDataFile]:
        """Prepare a file for batch processing."""
        timestamp = self._extract_timestamp(file_path)
        file_hash = self._calculate_file_hash(file_path)

        # Check if file needs processing
        already_processed, _ = await self.check_file_processed(
            timestamp, data_type, parameter, file_path
        )

        if already_processed:
            logger.info(f"File already processed and unchanged: {file_path}")
            return None

        return WeatherDataFile(
            timestamp=timestamp,
            data_type=data_type,
            parameter=parameter,
            model_data=model_data,
            file_path=file_path,
            file_hash=file_hash,
        )

    async def process_batch(self, files: List[WeatherDataFile]) -> BatchResult:
        """Process a batch of files in a single transaction."""
        if not self._pool:
            raise RuntimeError("Not connected")

        result = BatchResult()
        if not files:
            return result

        async with self._pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # Prepare bulk insert data
                    weather_data_records = [
                        (
                            f.timestamp,
                            f.data_type,
                            f.parameter,
                            json.dumps(f.model_data.model_dump(mode="json")),
                        )
                        for f in files
                    ]

                    metadata_records = [
                        (
                            f.timestamp,
                            f.data_type,
                            f.parameter,
                            str(f.file_path),
                            f.file_hash,
                        )
                        for f in files
                    ]

                    # Bulk insert weather data
                    await conn.executemany(
                        """
                        INSERT INTO raw_weather_data (
                            data_timestamp, data_type, parameter, validated_data
                        )
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (data_timestamp, data_type, parameter) 
                        DO UPDATE SET 
                            validated_data = EXCLUDED.validated_data,
                            ingestion_timestamp = NOW()
                    """,
                        weather_data_records,
                    )

                    # Bulk insert metadata
                    await conn.executemany(
                        """
                        INSERT INTO weather_data_metadata (
                            data_timestamp, data_type, parameter, file_path, file_hash
                        )
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (data_timestamp, data_type, parameter) 
                        DO UPDATE SET 
                            file_path = EXCLUDED.file_path,
                            file_hash = EXCLUDED.file_hash,
                            load_timestamp = NOW()
                    """,
                        metadata_records,
                    )

                    # Update metadata_cache with new entries
                    for file in files:
                        key = MetadataKey(
                            file.timestamp, file.data_type, file.parameter
                        )
                        entry = MetadataEntry(
                            str(file.file_path), file.file_hash, datetime.now()
                        )
                        self._metadata_cache[key] = entry

                    result.processed.extend(f.file_path for f in files)
                    logger.info(f"Successfully processed batch of {len(files)} files")

            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                result.failed.extend((f.file_path, str(e)) for f in files)

        return result

    async def store_data_batch(
        self, files_to_process: List[Tuple[str, str, BaseModel, Path]]
    ) -> BatchResult:
        """Process multiple files in batches."""
        total_result = BatchResult()
        current_batch: List[WeatherDataFile] = []

        for data_type, parameter, model_data, file_path in files_to_process:
            try:
                prepared_file = await self.prepare_file(
                    data_type, parameter, model_data, file_path
                )

                if prepared_file is None:
                    total_result.skipped.append(file_path)
                    continue

                current_batch.append(prepared_file)

                # Process batch if it reaches the size limit
                if len(current_batch) >= self.batch_size:
                    batch_result = await self.process_batch(current_batch)
                    self._merge_results(total_result, batch_result)
                    current_batch = []

            except Exception as e:
                logger.error(f"Error preparing file {file_path}: {e}")
                total_result.failed.append((file_path, str(e)))

        # Process remaining files
        if current_batch:
            batch_result = await self.process_batch(current_batch)
            self._merge_results(total_result, batch_result)

        return total_result

    def _merge_results(
        self, total_result: BatchResult, batch_result: BatchResult
    ) -> None:
        """Merge batch results into total results."""
        total_result.processed.extend(batch_result.processed)
        total_result.skipped.extend(batch_result.skipped)
        total_result.failed.extend(batch_result.failed)

    async def _initialize_metadata_cache(self) -> None:
        """Load all metadata into memory."""
        if not self._pool or self._cache_initialized:
            return

        logger.info("Initializing metadata cache...")
        async with self._pool.acquire() as conn:
            # Fetch all metadata in a single query
            rows = await conn.fetch("""
                SELECT
                    data_timestamp,
                    data_type,
                    parameter,
                    file_path,
                    file_hash,
                    load_timestamp
                FROM weather_data_metadata
            """)

        # Build cache
        self._metadata_cache.clear()
        for row in rows:
            key = MetadataKey(row["data_timestamp"], row["data_type"], row["parameter"])
            entry = MetadataEntry(
                row["file_path"], row["file_hash"], row["load_timestamp"]
            )
            self._metadata_cache[key] = entry

        self._cache_initialized = True
        logger.info(
            f"Metadata cache initialized with {len(self._metadata_cache)} entries"
        )
