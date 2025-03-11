#!/usr/bin/env python3
"""
Script to run the weather ETL pipeline.
"""

import argparse
from datetime import datetime, timedelta
import sys

from projects.sg_weather.pipelines.prefect_flows.elt_flow import weather_etl_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Run Weather ETL Pipeline")

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (default: 7 days before end-date)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (default: yesterday)",
    )

    parser.add_argument(
        "--parameters",
        type=str,
        nargs="+",
        help="Weather parameters to process (default: all available parameters)",
    )

    parser.add_argument(
        "--parallel",
        type=int,
        default=2,
        help="Number of parallel connections in pool (default: 2)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")

    end_date = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Run the pipeline
    result = weather_etl_pipeline(
        start_date=start_date,
        end_date=end_date,
        parameters=args.parameters,
        parallel=args.parallel,
    )

    print("\nPipeline Results:")
    print(f"Download: {result['download']}")
    print(f"Process: {result['process']}")
    print(f"Transform: {result['transform']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
