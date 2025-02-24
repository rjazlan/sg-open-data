#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import List, Tuple

from projects.sg_weather.config.settings import get_settings
from projects.sg_weather.src.ingestion.client import WeatherAPIClient
from projects.sg_weather.src.ingestion.storage import WeatherStorage
from projects.sg_weather.scripts.download_historical import download_date_range
from projects.sg_weather.scripts.load_to_database import load_historical_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def download_and_load_historical_data():
    """Download and load 3 years of historical weather data."""

    # Calculate date range (1 years from today)
    end_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    start_date = end_date - timedelta(days=365 * 1)

    # Define all parameters to download
    parameters = [
        "temperature",
        "rainfall",
        "humidity",
        "wind-speed",
        "wind-direction",
        "two-hour-forecast",
        "twenty-four-hour-forecast",
        "four-day-forecast",
        "pm25",
        "psi",
        "uv-index",
        # # "wbgt",
        # # "lightning",
    ]

    try:
        # Step 1: Download historical data
        logger.info(f"Downloading data from {start_date} to {end_date}")
        new_files, skipped = await download_date_range(
            start_date=start_date,
            end_date=end_date,
            parameters=parameters,
            force_download=False,  # Set to True to redownload existing data
        )
        logger.info(f"Download complete! New files: {new_files}, Skipped: {skipped}")

        # Step 2: Load data into database
        logger.info("Loading data into database")
        await load_historical_data()
        logger.info("Database load complete!")

    except Exception as e:
        logger.error(f"Error during download and load: {e}", exc_info=True)
        raise


async def verify_database_connection():
    """Verify database connection and create schema if needed."""
    storage = WeatherStorage(settings.database_url)
    try:
        await storage.connect()
        await storage.initialize_tables()
        logger.info("Database connection and schema verified")
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        await storage.disconnect()


if __name__ == "__main__":

    async def main():
        # First verify database connection
        await verify_database_connection()

        # Then download and load data
        await download_and_load_historical_data()

    asyncio.run(main())
