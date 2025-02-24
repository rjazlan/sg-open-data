# Singapore Weather Data ELT Pipeline Features

## Introduction

Hello, if you're like me and are overwhelmed every time a data portal gives you unabated access to their data system and you don't want to pay for ELT/ETL solutions (or suffer with setting up a whole Airflow system), you've come to the right place.

This is a ELT Pipeline that will pull all the data from the SG open data real-time weather APIs. You'll be pulling data to your local system and then inserting it all into your postgres instance. For me, I have a remote Postgres server sitting at home but you could remove the need for Postgres by commenting out the "Step 2" block in `scripts/historical_download_load.py` and the `verify_database_connection` function call in the main function.

This project will later consider using Airflow for orchestration but at the moment, it's just a simple workflow to pull, validate and store data.

Keep in mind these things:

- Downloading data: It just takes a while to pull gigabytes of data when you're looking at over a year worth of data
- Validating and storing data: Depending on where Postgres instance is based, it can take a while upsert data.

When using the pipeline, you might have different memory requirements so I suggest looking at `batch_size` inside `load_to_database.load_historical_data` -- the `batch_size` setting for `WeatherStorage` will control the number of files sent to your postgresql instance at once.

## Directory Structure

sg_weather/
├── data/
│ ├── raw/ # Raw JSON files by parameter
│ └── processed/ # Future transformed data  
├── src/
│ ├── ingestion/ # API client and storage
│ └── schemas/ # Pydantic models
└── scripts/ # Processing scripts

## Key Components

### 1. API Client

- Async request handling
- Rate limiting
- Connection pooling
- Error retry logic

### 2. Data models

- Base response validation
- Parameter-specific schemas
- Common component reuse
- Pythonic field names

### 3. Storage layer

- TimescaleDB integration
- Batch operation support
- Atomic updates
- State tracking

## Usage

Bear with me, the system was designed to be as friendly as possible but it's built for functionality foremost.

use the .env.example as a template for .env, enter your postgresql details. This will give you the ability to connect to your database. Otherwise, just install the dependencies using the pyproject.toml / requirements.lock. I use Rye (rust-based package manager) because I don't have to wait 15 minutes for package resolution. Make sure to activate your environment.

```
python -m projects.sg_weather.scripts.historical_download_load
```

The current download range is set to download all data in the last year from yesterday. You can change this by settings the `start_date` and `end_date` variables in the `historical_download_load` script. Just keep in mind that each day has roughly 4 MB of data, a year 1.5 GB. It might take a while to downlaod and sync. That being said, system metadata exists so you can just run the script again; it will scan for what you are missing and then pull, validate and store.
