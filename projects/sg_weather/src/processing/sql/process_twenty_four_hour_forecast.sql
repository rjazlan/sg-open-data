-- Insert regional forecasts
INSERT INTO weather_transformed.twentyfour_hour_regional_forecasts (
    timestamp, period_start, period_end, region, forecast_text, forecast_code
)
WITH regional_data AS (
    SELECT
        (record->>'timestamp')::TIMESTAMPTZ AS timestamp,
        (period->'time_period'->>'start')::TIMESTAMPTZ AS period_start,
        (period->'time_period'->>'end')::TIMESTAMPTZ AS period_end,
        unnest(ARRAY['west', 'east', 'north', 'south', 'central']) AS region,
        unnest(ARRAY[
            period->'regions'->'west'->>'text',
            period->'regions'->'east'->>'text',
            period->'regions'->'north'->>'text',
            period->'regions'->'south'->>'text',
            period->'regions'->'central'->>'text'
        ]) AS forecast_text,
        unnest(ARRAY[
            period->'regions'->'west'->>'code',
            period->'regions'->'east'->>'code',
            period->'regions'->'north'->>'code',
            period->'regions'->'south'->>'code',
            period->'regions'->'central'->>'code'
        ]) AS forecast_code
    FROM 
        raw_weather_data r,
        jsonb_array_elements(r.validated_data->'records') AS record,
        jsonb_array_elements(record->'periods') AS period
    WHERE 
        r.parameter = 'twenty-four-hour-forecast'
        AND DATE(r.data_timestamp) = $1
)
SELECT * FROM regional_data
ON CONFLICT (timestamp, region, period_start) DO UPDATE SET
    period_end = EXCLUDED.period_end,
    forecast_text = EXCLUDED.forecast_text,
    forecast_code = EXCLUDED.forecast_code;

-- Insert general forecasts
INSERT INTO weather_transformed.twentyfour_hour_general_forecasts (
    timestamp, period_start, period_end, 
    temperature_low, temperature_high, humidity_low, humidity_high,
    wind_direction, wind_speed_low, wind_speed_high, 
    forecast_text, forecast_code
)
SELECT
    (record->>'timestamp')::TIMESTAMPTZ AS timestamp,
    (record->'general'->'valid_period'->>'start')::TIMESTAMPTZ AS period_start,
    (record->'general'->'valid_period'->>'end')::TIMESTAMPTZ AS period_end,
    (record->'general'->'temperature'->>'low')::FLOAT AS temperature_low,
    (record->'general'->'temperature'->>'high')::FLOAT AS temperature_high,
    (record->'general'->'relative_humidity'->>'low')::FLOAT AS humidity_low,
    (record->'general'->'relative_humidity'->>'high')::FLOAT AS humidity_high,
    record->'general'->'wind'->>'direction' AS wind_direction,
    (record->'general'->'wind'->'speed'->>'low')::FLOAT AS wind_speed_low,
    (record->'general'->'wind'->'speed'->>'high')::FLOAT AS wind_speed_high,
    record->'general'->'forecast'->>'text' AS forecast_text,
    record->'general'->'forecast'->>'code' AS forecast_code
FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'records') AS record
WHERE 
    r.parameter = 'twenty-four-hour-forecast'
    AND DATE(r.data_timestamp) = $1
ON CONFLICT (timestamp, period_start) DO UPDATE SET
    period_end = EXCLUDED.period_end,
    temperature_low = EXCLUDED.temperature_low,
    temperature_high = EXCLUDED.temperature_high,
    humidity_low = EXCLUDED.humidity_low,
    humidity_high = EXCLUDED.humidity_high,
    wind_direction = EXCLUDED.wind_direction,
    wind_speed_low = EXCLUDED.wind_speed_low,
    wind_speed_high = EXCLUDED.wind_speed_high,
    forecast_text = EXCLUDED.forecast_text,
    forecast_code = EXCLUDED.forecast_code;

