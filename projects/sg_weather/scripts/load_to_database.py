import asyncio
import json
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import logging

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
    # UVIndexResponse,
    # WeatherResponse,
    # PM25Response,
    # PSIResponse,
    # TwoHourForecastResponse,
    # TwentyFourHourOutlookResponse,
    # FourDayOutlookResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


async def load_historical_data() -> None:
    storage = WeatherStorage(settings.database_url, batch_size=300)
    await storage.connect()
    await storage.initialize_tables()

    try:
        files_to_process = []

        for param_dir in settings.RAW_DATA_DIR.glob("*"):
            if not param_dir.is_dir():
                continue

            parameter = param_dir.name
            print(f"\nProcessing {parameter}...")

            json_files = list(param_dir.glob("*.json"))
            for i in range(0, len(json_files), 10):
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

                            # await storage.store_data(data_type, parameter, model_data, file)
                            files_to_process.append(
                                (data_type, parameter, model_data, file)
                            )

                    except Exception as e:
                        print(f"Error loading {file}: {e}")

        # Process all files in batches
        result = await storage.store_data_batch(files_to_process)

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


if __name__ == "__main__":
    asyncio.run(load_historical_data())
