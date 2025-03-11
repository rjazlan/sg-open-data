from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import asyncio
from datetime import datetime, timedelta
import logging
import mlflow

from projects.sg_weather.src.ingestion.downloader import (
    download_weather_data_for_period,
)
from projects.sg_weather.src.ingestion.processor import process_and_load_raw_files
from projects.sg_weather.src.processing.transform import transform_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(name="Download Weather Data", retries=3, retry_delay_seconds=60)
async def download_task(start_date, end_date, parameters):
    """Task to download weather data for a period"""
    logger.info(f"Downloading weather data from {start_date} to {end_date}")
    logger.info(f"Parameters: {parameters}")

    new_files, skipped = await download_weather_data_for_period(
        start_date=start_date,
        end_date=end_date,
        parameters=parameters,
        force_download=False,
    )

    return {"new_files": new_files, "skipped": skipped}


@task(name="Process Raw Files")
async def process_task(start_date, end_date, parameters=None):
    """Task to process and load raw files into database"""
    logger.info("Processing raw files and loading to database")

    stats = await process_and_load_raw_files(
        start_date, end_date, parameters=parameters
    )

    return stats


@task(name="Transform Weather Data")
async def transform_task(start_date=None, end_date=None, parallel=3):
    """Task to transform raw data into structured tables"""
    logger.info("Transforming raw data into structured tables")

    results = await transform_data(start_date, end_date, parallel)

    return results


@task(name="Log to MLflow")
def log_to_mlflow(download_stats, process_stats, transform_stats):
    """Log metrics to MLflow"""
    # Set experiment
    mlflow.set_experiment("weather_etl_pipeline")

    # Start run
    with mlflow.start_run(
        run_name=f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ):
        # Log download metrics
        mlflow.log_metric("downloaded_files", download_stats["new_files"])
        mlflow.log_metric("skipped_files", download_stats["skipped"])

        # Log processing metrics
        mlflow.log_metric("processed_files", process_stats["processed"])
        mlflow.log_metric("processing_skipped", process_stats["skipped"])
        mlflow.log_metric("processing_failed", process_stats["failed"])

        # Log transform metrics
        for table, count in transform_stats.items():
            mlflow.log_metric(f"transformed_{table}_count", count)

        # Log parameters
        mlflow.log_param("etl_timestamp", datetime.now().isoformat())

    return True


@flow(name="Weather ETL Pipeline", task_runner=ConcurrentTaskRunner())
def weather_etl_pipeline(start_date=None, end_date=None, parameters=None, parallel=2):
    """Main ETL flow for weather data pipeline"""
    # Default to yesterday if no dates provided
    if not end_date:
        end_date = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    if not start_date:
        start_date = end_date - timedelta(days=7)  # Default to 1 week

    if not parameters:
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
        ]

    # Convert string dates to datetime if needed
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace("Z", "+08:00"))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace("Z", "+08:00"))
    logger.info(f"Running pipeline from {start_date} and {end_date}")
    # Execute download task
    download_stats = asyncio.run(download_task(start_date, end_date, parameters))

    # Execute process task
    process_stats = asyncio.run(process_task(start_date, end_date, parameters))

    # Execute transform task
    transform_stats = asyncio.run(transform_task(start_date, end_date, parallel))

    # Log to MLflow
    log_to_mlflow(download_stats, process_stats, transform_stats)

    return {
        "download": download_stats,
        "process": process_stats,
        "transform": transform_stats,
    }
