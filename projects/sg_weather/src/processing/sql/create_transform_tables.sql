-- Drop schema if it exists to ensure clean slate
DROP SCHEMA IF EXISTS weather_transformed CASCADE;

-- Create schema for transformed data
CREATE SCHEMA weather_transformed;

-- Create tables
CREATE TABLE weather_transformed.stations (
    station_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    region TEXT,
    device_id TEXT
);

CREATE TABLE weather_transformed.regions (
    region_id TEXT PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

CREATE TABLE weather_transformed.areas (
    name TEXT PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

CREATE TABLE weather_transformed.weather_measurements (
    measurement_id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL,
    parameter TEXT NOT NULL,
    value FLOAT NOT NULL,
    units TEXT NOT NULL,
    PRIMARY KEY (measurement_id, timestamp)
);

CREATE TABLE weather_transformed.national_measurements (
    measurement_id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    parameter TEXT NOT NULL,
    value FLOAT NOT NULL,
    units TEXT NOT NULL,
    PRIMARY KEY (measurement_id, timestamp)
);

CREATE TABLE IF NOT EXISTS weather_transformed.two_hour_forecasts (
    forecast_id BIGSERIAL ,
    timestamp TIMESTAMPTZ NOT NULL,   -- When forecast was issued
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    area TEXT NOT NULL,
    forecast_text TEXT NOT NULL,
    forecast_code TEXT,
    PRIMARY KEY (forecast_id, timestamp),
    UNIQUE(timestamp, area)
);

CREATE TABLE IF NOT EXISTS weather_transformed.twentyfour_hour_regional_forecasts (
    forecast_id BIGSERIAL ,
    timestamp TIMESTAMPTZ NOT NULL,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    region TEXT NOT NULL,
    forecast_text TEXT NOT NULL,
    forecast_code TEXT,
    PRIMARY KEY (forecast_id, timestamp),
    UNIQUE(timestamp, region, period_start)
);

CREATE TABLE IF NOT EXISTS weather_transformed.twentyfour_hour_general_forecasts (
    forecast_id BIGSERIAL ,
    timestamp TIMESTAMPTZ NOT NULL,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    temperature_low FLOAT,
    temperature_high FLOAT,
    humidity_low FLOAT,
    humidity_high FLOAT,
    wind_direction TEXT,
    wind_speed_low FLOAT,
    wind_speed_high FLOAT,
    forecast_text TEXT NOT NULL,
    forecast_code TEXT,
    PRIMARY KEY (forecast_id, timestamp),
    UNIQUE(timestamp, period_start)
);

CREATE TABLE IF NOT EXISTS weather_transformed.four_day_forecasts (
    forecast_id BIGSERIAL ,
    timestamp TIMESTAMPTZ NOT NULL,
    forecast_date TIMESTAMPTZ NOT NULL,
    day_of_week TEXT,
    temperature_low FLOAT,
    temperature_high FLOAT,
    humidity_low FLOAT,
    humidity_high FLOAT,
    wind_direction TEXT,
    wind_speed_low FLOAT,
    wind_speed_high FLOAT,
    forecast_text TEXT NOT NULL,
    forecast_code TEXT,
    PRIMARY KEY (forecast_id, timestamp),
    UNIQUE(timestamp, forecast_date)
);

-- Convert to hypertables
select
    create_hypertable(
        'weather_transformed.weather_measurements', 'timestamp', if_not_exists => true
    )
;
select
    create_hypertable(
        'weather_transformed.national_measurements', 'timestamp', if_not_exists => true
    )
;
select
    create_hypertable(
        'weather_transformed.two_hour_forecasts', 'timestamp', if_not_exists => true
    )
;
select
    create_hypertable(
        'weather_transformed.twentyfour_hour_regional_forecasts',
        'timestamp',
        if_not_exists => true
    )
;
select
    create_hypertable(
        'weather_transformed.twentyfour_hour_general_forecasts',
        'timestamp',
        if_not_exists => true
    )
;
select
    create_hypertable(
        'weather_transformed.four_day_forecasts', 'timestamp', if_not_exists => true
    )
;
-- Add unique constraints (separate from primary key)
create unique index idx_weather_measurements_unique
    on weather_transformed.weather_measurements(timestamp, station_id, parameter)
;

CREATE UNIQUE INDEX idx_national_measurements_unique 
ON weather_transformed.national_measurements (timestamp, parameter);

CREATE INDEX idx_two_hour_forecasts_area ON weather_transformed.two_hour_forecasts(area);
CREATE INDEX idx_two_hour_forecasts_validity ON weather_transformed.two_hour_forecasts(valid_from, valid_to);

CREATE INDEX idx_24h_regional_region ON weather_transformed.twentyfour_hour_regional_forecasts(region);
CREATE INDEX idx_24h_regional_period ON weather_transformed.twentyfour_hour_regional_forecasts(period_start, period_end);

CREATE INDEX idx_24h_general_period ON weather_transformed.twentyfour_hour_general_forecasts(period_start, period_end);
CREATE INDEX idx_24h_general_temperature ON weather_transformed.twentyfour_hour_general_forecasts(temperature_high, temperature_low);

CREATE INDEX idx_4day_date ON weather_transformed.four_day_forecasts(forecast_date);
CREATE INDEX idx_4day_temp ON weather_transformed.four_day_forecasts(temperature_high, temperature_low);

-- Create metadata tracking table
CREATE TABLE weather_transformed.transform_metadata (
    date DATE PRIMARY KEY,
    parameters JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL
);

