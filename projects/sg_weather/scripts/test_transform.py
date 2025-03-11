#!/usr/bin/env python3
import asyncio
import argparse
from datetime import datetime
from projects.sg_weather.src.processing.transform import transform_data


async def run_transform_test(start_date=None, end_date=None, parallel=3):
    """Run transformation with optional date filtering."""
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    print(
        f"Starting transformation test: {start_date} to {end_date} with {parallel} parallel processes"
    )
    await transform_data(start_date, end_date, parallel)
    print("Transformation test completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test weather data transformation")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--parallel", type=int, default=3, help="Number of dates to process in parallel"
    )
    args = parser.parse_args()

    asyncio.run(run_transform_test(args.start_date, args.end_date, args.parallel))
