import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Callable, Any
from tqdm import tqdm

from projects.sg_weather.config.settings import get_settings
from projects.sg_weather.src.ingestion.client import WeatherAPIClient


settings = get_settings()

DATA_DIR = settings.DATA_DIR
RAW_DATA_DIR = settings.RAW_DATA_DIR


class DownloadMetadata:
    """Manages download state metadata efficiently in O(1) lookup time"""

    def __init__(self, metadata_file: Path = DATA_DIR / "download_metadata.json"):
        self.metadata_file = metadata_file
        self.data: Dict[str, Dict[str, str]] = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Dict[str, str]]:
        """Load metadata from file or create new."""
        # Define all expected fields
        expected_fields = {
            "temperature": {},
            "rainfall": {},
            "humidity": {},
            "wind-speed": {},
            "wind-direction": {},
            "two-hour-forecast": {},
            "twenty-four-hour-forecast": {},
            "four-day-forecast": {},
            "wbgt": {},
            "lightning": {},
            "pm25": {},
            "psi": {},
            "uv-index": {},
        }

        if self.metadata_file.exists():
            with open(self.metadata_file) as f:
                existing_metadata = json.load(f)

                # Check for and add any missing fields
                for field in expected_fields:
                    if field not in existing_metadata:
                        existing_metadata[field] = {}

                return existing_metadata

        return expected_fields

    def save(self) -> None:
        """Save metadata to file."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.data, f)

    def is_downloaded(self, param: str, date: datetime) -> bool:
        """Check if a specific date-parameter combination exists."""
        date_str = date.strftime("%Y%m%d")
        return date_str in self.data[param]

    def mark_downloaded(self, param: str, date: datetime) -> None:
        """Mark a date-parameter combination as downloaded."""
        date_str = date.strftime("%Y%m%d")
        self.data[param][date_str] = datetime.now().isoformat()


async def download_date_range(
    start_date: datetime,
    end_date: datetime,
    parameters: List[str],
    force_download: bool = False,
) -> Tuple[int, int]:
    """Download weather data for a date range."""

    metadata = DownloadMetadata()
    new_files = 0
    skipped_files = 0

    # Define method mapping with additional endpoints
    METHOD_MAP = {
        "temperature": lambda client: client.get_temperature,
        "rainfall": lambda client: client.get_rainfall,
        "humidity": lambda client: client.get_humidity,
        "wind-speed": lambda client: client.get_wind_speed,
        "wind-direction": lambda client: client.get_wind_direction,
        "two-hour-forecast": lambda client: client.get_two_hour_forecast,
        "twenty-four-hour-forecast": lambda client: client.get_24_hour_forecast,
        "four-day-forecast": lambda client: client.get_four_day_forecast,
        "wbgt": lambda client: client.get_wbgt,
        "lightning": lambda client: client.get_lightning,
        "pm25": lambda client: client.get_pm25,
        "psi": lambda client: client.get_psi,
        "uv-index": lambda client: client.get_uv_index,
    }

    async def save_response(client: WeatherAPIClient, date: datetime, param: str):
        nonlocal new_files, skipped_files

        # Quick metadata check unless forcing download
        if not force_download and metadata.is_downloaded(param, date):
            skipped_files += 1
            return

        if param not in METHOD_MAP:
            print(f"Warning: Unknown parameter {param}")
            return

        try:
            method = METHOD_MAP[param](client)
            response = await method(date)

            # Print raw response before saving
            # print(f"\nRaw response for {param} on {date}:")
            # print(json.dumps(response.model_dump(), indent=2)[:500])

            # Create parameter-specific directory
            param_dir = RAW_DATA_DIR / param
            param_dir.mkdir(exist_ok=True)

            # Save to file
            filename = param_dir / f"{date.strftime('%Y%m%d')}_{param}.json"
            with open(filename, "w") as f:
                json.dump(
                    response.model_dump(by_alias=True, mode="json"), f, default=str
                )

            # Update metadata
            metadata.mark_downloaded(param, date)
            new_files += 1

        except Exception as e:
            print(f"\nError saving {param} data for {date}:")
            print(f"Error type: {type(e)}")
            print(f"Error message: {str(e)}")
            if hasattr(e, "__dict__"):
                print(f"Error details: {e.__dict__}")

    async with WeatherAPIClient() as client:
        total_days = (end_date - start_date).days + 1
        date_range = [start_date + timedelta(days=x) for x in range(total_days)]

        # Validate parameters
        valid_params = [p for p in parameters if p in METHOD_MAP]
        if len(valid_params) != len(parameters):
            invalid_params = set(parameters) - set(valid_params)
            print(f"Warning: Skipping invalid parameters: {invalid_params}")

        for current_date in tqdm(date_range, desc="Downloading data"):
            tasks = []
            for param in valid_params:
                tasks.append(save_response(client, current_date, param))

            await asyncio.gather(*tasks)
            metadata.save()  # Save after each day
            # await asyncio.sleep(1)  # Rate limiting

    return new_files, skipped_files


async def test_download():
    # Test with multiple parameters
    start_date = datetime(2024, 12, 1)
    end_date = datetime(2024, 12, 7)  # Just one day for testing

    # Test with multiple parameters including new endpoints
    test_parameters = [
        "temperature",
        # "rainfall",
        # "humidity",
        # "wind-speed",
        # "wind-direction",
        # "two-hour-forecast",
        # "twenty-four-hour-forecast",
        # "four-day-forecast",
        # # "wbgt",
        # # "lightning",
        # "pm25",
        # "psi",
        # "uv-index",
    ]

    print(f"Testing download from {start_date} to {end_date}")
    new_files, skipped = await download_date_range(
        start_date,
        end_date,
        parameters=test_parameters,
    )
    print(f"Download complete! New files: {new_files}, Skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(test_download())
