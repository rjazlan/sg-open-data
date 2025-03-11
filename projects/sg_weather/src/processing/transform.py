#!/usr/bin/env python3
import asyncio
import asyncpg
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from projects.sg_weather.config.settings import get_settings
from datetime import datetime, timedelta
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()

SQL_DIR = Path(__file__).parent / "sql"


async def execute_sql_file(conn, file_path, params={}):
    """Execute SQL statements from a file."""
    logger.info(f"Executing SQL file: {file_path}")
    with open(file_path, "r") as f:
        sql = f.read()

    # Split by semicolon to handle multiple statements
    statements = sql.split(";")
    for statement in statements:
        statement = statement.strip()
        if statement:
            try:
                await conn.execute(statement, *params.values())
            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                logger.error(f"Statement: {statement}")
                raise


async def create_transform_metadata_table(pool):
    """Create metadata table to track transformation progress."""
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_transformed.transform_metadata (
            date DATE PRIMARY KEY,
            parameters JSON NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status TEXT NOT NULL
        )
        """)


async def check_date_for_processing(pool, date):
    """
    Check if a date needs processing or reprocessing
    """
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT status FROM weather_transformed.transform_metadata WHERE date = $1",
            date,
        )
        if not record:
            return True
        return record["status"] != "completed"


async def mark_date_status(pool, date, status, parameters=None):
    """Mark a date's processing status in metadata."""
    async with pool.acquire() as conn:
        if status == "started":
            # Convert parameters list to JSONB
            params_json = json.dumps(parameters) if parameters else "{}"

            await conn.execute(
                """
            INSERT INTO weather_transformed.transform_metadata
            (date, parameters, started_at, status)
            VALUES ($1, $2, NOW(), $3)
            ON CONFLICT (date) DO UPDATE SET
            started_at = NOW(),
            parameters = $2::jsonb,
            status = $3
            """,
                date,
                params_json,
                status,
            )
        else:
            await conn.execute(
                """
            UPDATE weather_transformed.transform_metadata
            SET completed_at = NOW(), status = $1
            WHERE date = $2
            """,
                status,
                date,
            )


async def transform_date_measurements(pool, date):
    """
    Transform measurements for a specific date.
    """
    logger.info(f"Processing measurements for date: {date}")

    # Track available parameters for this date
    async with pool.acquire() as conn:
        parameters = await conn.fetch(
            """
        SELECT DISTINCT parameter
        FROM raw_weather_data
        WHERE DATE(data_timestamp) = $1
        """,
            date,
        )
        param_list = [row["parameter"] for row in parameters]

    # Mark date as started in metadata
    await mark_date_status(pool, date, "started", param_list)

    try:
        async with pool.acquire() as conn:
            # Process station-based measurements
            await execute_sql_file(
                conn, SQL_DIR / "process_station_measurements.sql", {"date": date}
            )

            # Process PM2.5
            await execute_sql_file(conn, SQL_DIR / "process_pm25.sql", {"date": date})

            # Process PSI with sub-indexes
            await execute_sql_file(conn, SQL_DIR / "process_psi.sql", {"date": date})

            # Process national measurements (UV index)
            await execute_sql_file(
                conn, SQL_DIR / "process_uv_index.sql", {"date": date}
            )

            # Process 2 hour forecasts
            await execute_sql_file(
                conn, SQL_DIR / "process_two_hour_forecast.sql", {"date": date}
            )

            # Process 24 hour forecasts
            await execute_sql_file(
                conn, SQL_DIR / "process_twenty_four_hour_forecast.sql", {"date": date}
            )

            # Process 4 day forecasts
            await execute_sql_file(
                conn, SQL_DIR / "process_four_day_forecast.sql", {"date": date}
            )

        await mark_date_status(pool, date, "completed")
        logger.info(f"Completed processing date: {date}")
        return True
    except Exception as e:
        logger.error(f"Error processing date {date}: {e}")
        await mark_date_status(pool, date, "failed")
        return False


async def transform_data(start_date=None, end_date=None, parallel=3):
    """Transform raw weather data into structured tables with date-based tracking"""
    logger.info("Starting data transformation process")
    transform_stats = {}

    # Ensure SQL directory exists
    SQL_DIR.mkdir(exist_ok=True, parents=True)

    # Create connection pool
    pool = await asyncpg.create_pool(
        settings.database_url, min_size=2, max_size=parallel + 2
    )

    try:
        # Create schema and tables first
        async with pool.acquire() as conn:
            await execute_sql_file(conn, SQL_DIR / "create_transform_tables.sql")

            # Get date range if not specified
            if not start_date or not end_date:
                date_range = await conn.fetch("""
                SELECT 
                    MIN(DATE(data_timestamp)) as min_date,
                    MAX(DATE(data_timestamp)) as max_date
                FROM raw_weather_data
                """)
                if (
                    date_range
                    and date_range[0]["min_date"]
                    and date_range[0]["max_date"]
                ):
                    start_date = (
                        date_range[0]["min_date"] if not start_date else start_date
                    )
                    end_date = date_range[0]["max_date"] if not end_date else end_date

        # Create metadata table
        await create_transform_metadata_table(pool)

        # Populate stations and regions (these are done once for all dates)
        async with pool.acquire() as conn:
            await execute_sql_file(conn, SQL_DIR / "process_stations.sql")
            await execute_sql_file(conn, SQL_DIR / "process_regions.sql")
            await execute_sql_file(conn, SQL_DIR / "process_areas.sql")

        # Generate date range
        processed_dates = 0
        skipped_dates = 0
        if start_date and end_date:
            current_date = start_date
            dates_to_process = []
            while current_date <= end_date:
                if await check_date_for_processing(pool, current_date):
                    dates_to_process.append(current_date)
                else:
                    skipped_dates += 1
                current_date += timedelta(days=1)

            logger.info(
                f"Found {len(dates_to_process)} dates to process from {start_date} to {end_date}"
            )

            # Process dates in parallel batches
            for i in range(0, len(dates_to_process), parallel):
                batch = dates_to_process[i : i + parallel]
                tasks = [transform_date_measurements(pool, date) for date in batch]
                results = await asyncio.gather(*tasks)
                processed_dates = sum(1 for r in results if r)

        # Get final counts for transformed tasks
        tables_to_count = [
            # "stations",
            # "regions",
            # "areas",
            "weather_measurements",
            "national_measurements",
            "two_hour_forecasts",
            "twentyfour_hour_general_forecasts",
            "twentyfour_hour_regional_forecasts",
            "four_day_forecasts",
        ]

        # Collect counts for all tables
        async with pool.acquire() as conn:
            for table in tables_to_count:
                try:
                    # Build dynamic query based on available date parameters
                    if start_date and end_date:
                        query = f"SELECT COUNT(*) FROM weather_transformed.{table} WHERE timestamp BETWEEN $1::date AND $2::date"
                        params = [start_date, end_date]
                    elif start_date:
                        query = f"SELECT COUNT(*) FROM weather_transformed.{table} WHERE timestamp >= $1::date"
                        params = [start_date]
                    elif end_date:
                        query = f"SELECT COUNT(*) FROM weather_transformed.{table} WHERE timestamp <= $1::date"
                        params = [end_date]
                    else:
                        query = f"SELECT COUNT(*) FROM weather_transformed.{table}"
                        params = []
                        transform_stats[table] = await conn.fetchval(
                            f"SELECT COUNT(*) FROM weather_transformed.{table} "
                        )
                    # Execute the appropriate query
                    transform_stats[table] = await conn.fetchval(query, *params)
                except Exception as e:
                    logger.warning(f"Error counting table {table}: {e}")
                    transform_stats[table] = 0
            transform_stats["processed_dates"] = processed_dates
            transform_stats["skipped_dates"] = skipped_dates

        logger.info(f"Data transformation completed: {transform_stats}")
        return transform_stats
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(transform_data())
