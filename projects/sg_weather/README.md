# Singapore Weather Data ELT Pipeline Features

## Introduction

Hello, if you're like me and are overwhelmed every time a data portal gives you unabated access to their data system and you don't want to pay for ELT/ETL solutions (or suffer with setting up a whole Airflow system / though I'm trying to tell myself we can just use postgres for 90% of the tech stack.), you've come to the right place.

This is an ELT pipeline that will pull all the data from the SG open data real-time weather APIs, You'll be pulling data to your local system (This is because Python needs to validate the data, and I haven't set up any remote execution if you're working with remote servers) and then pushing it to a PostgreSQL instance. There is metadata tracking to prevent unnecessary downloads if you've run the pipeline before.

This project will later consider using Airflow for orchestration but at the moment, it's just a simple workflow to pull, validate and store data using Prefect and MLflow for orchestration and tracking.

Keep in mind these things:

- Downloading data: It just takes a while to pull gigabytes of data when you're looking at over a year worth of data
- Validating and storing data: Depending on where Postgres instance is based, it can take a while upsert data.
- Transforming raw data into tables: This can take a while -- the code is set to process the data in daily batches using parallelization in a connection pool to postgres.

When using the pipeline, you might have different memory requirements so I suggest looking at `batch_size` inside `processor.process_and_load_raw_files` -- the `batch_size` setting for `WeatherStorage` will control the number of files sent to your postgresql instance at once.

## Key Components

### 1. API Client

- Async request handling
- Rate limiting
- Connection pooling
- Error retry logic

### 2. Pydantic Data models

- Base response validation
- Parameter-specific schemas
- Common component reuse
- Pythonic field names

### 3. Storage layer

- TimescaleDB integration
- Batch operation support
- Atomic updates
- State tracking

### 4. PostgreSQL Data Tables

The pipeline creates several structured tables:

- Stations: Location and metadata for weather stations
- Weather Measurements: Time-series of station measurements
- Two-Hour Forecasts: Short-term area-specific predictions
- 24-Hour Forecasts: Regional and general daily forecasts
- Four-Day Forecasts: Extended outlook with meteorological data

## Usage

Bear with me, the system was designed to be as friendly as possible but it's built for functionality foremost.

use the .env.example as a template for .env, enter your postgresql details. This will give you the ability to connect to your database. Otherwise, just install the dependencies using the pyproject.toml / requirements.lock. I use Rye (rust-based package manager) because I don't have to wait 15 minutes for package resolution. Make sure to activate your environment.

1.

```bash
cp .env.template .env
# Edit .env with your database credentials
```

2.

```bash
python -m projects.sg_weather.scripts.run_elt_pipeline --start_date <date> --end_date <date> --parallel 2
```

Parameters:
--start-date: First date to process (YYYY-MM-DD)
--end-date: Last date to process (YYYY-MM-DD)
--parallel: Number of dates to process simultaneously

### Performance

- Each day contains ~4MB of data (~1.5GB/year)
- Download speed depends on network connection
- Processing speed depends on database location and resources
- Adjust parallel and batch_size based on available memory

## Data resources

The pipeline collects data from these API endpoints:

- Temperature, rainfall, relative humidity, wind measurements
- Two-hour, 24-hour, 4-day forecasts
- PM2.5, PSI, UV index readings

## Future work

We'll be doing EDA, clustering and time-series analysis to look at how different areas in Singapore experience heat. We'll then be incorporating some map data and development plans from the URA to look at the effect of building density in certain areas on microclimates
