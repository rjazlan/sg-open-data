import asyncio
import json
from pathlib import Path
from platform import processor
from tqdm import tqdm
from datetime import datetime
import logging
from typing import Dict, List, Optional

from projects.sg_weather.config.settings import get_settings
from projects.sg_weather.src.ingestion.storage import WeatherStorage
from projects.sg_weather.src.schemas.weather import (
    UVIndexData,
    WeatherReadingData,
    PM25Data,
    PSIData,
    TwoHourForecastData,
    TwentyFourHourOutlookData,
    FourDayOutlookData,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def process_and_load_raw_files(
    start_date: datetime,
    end_date: datetime,
    parameters: Optional[List[str]] = None,
    batch_size: int = 300,
) -> Dict[str, int]:
    storage = WeatherStorage(settings.database_url, batch_size=300)
    await storage.connect()
    await storage.initialize_tables()

    stats = {"processed": 0, "skipped": 0, "failed": 0}

    try:
        files_to_process = []

        # Filter directories by parameter if specified
        param_dirs = settings.RAW_DATA_DIR.glob("*")
        if parameters:
            param_dirs = [d for d in param_dirs if d.name in parameters and d.is_dir()]
        else:
            param_dirs = [d for d in param_dirs if d.is_dir()]

        for param_dir in param_dirs:
            parameter = param_dir.name
            print(f"\nProcessing {parameter}...")

            json_files = list(param_dir.glob("*.json"))

            # Filter files by date range
            date_filtered_files = []
            for file in json_files:
                # Extract date from filename (YYYYMMDD-parameter.json)
                try:
                    date_str = file.stem.split("_")[0]
                    file_date = datetime.strptime(date_str, "%Y%m%d")

                    # Check if file date is within the specified range
                    if start_date <= file_date <= end_date:
                        date_filtered_files.append(file)
                except (ValueError, IndexError):
                    logger.warning(f"Couldn't parse date from filename: {file}")

            logger.info(
                f"Found {len(date_filtered_files)} files within date range for {parameter}"
            )
            for i in range(0, len(date_filtered_files), 10):
                chunk = json_files[i : i + 10]

                for file in chunk:
                    try:
                        with open(file) as f:
                            data = json.load(f)

                            # Use appropriate Pydantic model based on parameter
                            if parameter in [
                                "temperature",
                                "rainfall",
                                "humidity",
                                "wind-direction",
                                "wind-speed",
                            ]:
                                model_data = WeatherReadingData(**data)
                                data_type = "reading"
                            elif parameter == "uv-index":
                                model_data = UVIndexData(**data)
                                data_type = "reading"
                            elif parameter == "pm25":
                                model_data = PM25Data(**data)
                                data_type = "reading"
                            elif parameter == "psi":
                                model_data = PSIData(**data)
                                data_type = "reading"
                            elif parameter == "two-hour-forecast":
                                model_data = TwoHourForecastData(**data)
                                data_type = "forecast"
                            elif parameter == "twenty-four-hour-forecast":
                                model_data = TwentyFourHourOutlookData(**data)
                                data_type = "forecast"
                            elif parameter == "four-day-forecast":
                                model_data = FourDayOutlookData(**data)
                                data_type = "forecast"
                            else:
                                logger.warning(
                                    f"Unsupported parameter: {parameter}, skipping file: {file}"
                                )

                            # await storage.store_data(data_type, parameter, model_data, file)
                            files_to_process.append(
                                (data_type, parameter, model_data, file)
                            )

                    except Exception as e:
                        print(f"Error loading {file}: {e}")
                        stats["failed"] += 1

        # Process all files in batches
        logger.info(
            f"Preparing to process {len(files_to_process)} files in batches of {batch_size}"
        )
        result = await storage.store_data_batch(files_to_process)

        # Update stats
        stats["processed"] = len(result.processed)
        stats["skipped"] += len(result.skipped)
        stats["failed"] += len(result.failed)

        # Log results
        logger.info("\nBatch Processing Results:")
        logger.info(f" ✓ Successfully processed: {len(result.processed)} files")
        logger.info(f" ✓ Skipped (unchanged): {len(result.skipped)} files")
        logger.info(f" ✓ Failed: {len(result.failed)} files")

        if result.failed:
            logger.info("\n ✕ Failed files:")
            for file_path, error in result.failed:
                logger.info(f"- {file_path}: {error}")

    finally:
        await storage.disconnect()

    return stats


if __name__ == "__main__":
    # Example usage with date range
    from datetime import datetime, timedelta

    # Process last 7 days
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=7)

    # Optionally specify parameters to process
    parameters = ["temperature", "rainfall", "humidity"]

    asyncio.run(
        process_and_load_raw_files(
            start_date=start_date, end_date=end_date, parameters=None
        )
    )
