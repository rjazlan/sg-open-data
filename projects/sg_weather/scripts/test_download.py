# Test script for a 3-day period
from datetime import datetime
import asyncio
from projects.sg_weather.scripts.download_historical import download_date_range


async def test_download():
    start_date = datetime(2024, 12, 1)
    end_date = datetime(2024, 12, 1)  # Just 3 days to test

    print(f"Testing download from {start_date} to {end_date}")
    new_files, skipped = await download_date_range(
        start_date,
        end_date,
        # parameters=["temperature", "rainfall"],  # Let's start with just 2 parameters
        parameters=[
            "temperature",
            "rainfall",
            "humidity",
            "wind-speed",
            "wind-direction",
        ],
    )
    print(f"Download complete! New files: {new_files}, Skipped: {skipped}")


if __name__ == "__main__":
    asyncio.run(test_download())
