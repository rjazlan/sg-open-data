INSERT INTO weather_transformed.four_day_forecasts (
    timestamp, forecast_date, day_of_week,
    temperature_low, temperature_high, humidity_low, humidity_high,
    wind_direction, wind_speed_low, wind_speed_high, 
    forecast_text, forecast_code
)
SELECT
    (record->>'timestamp')::TIMESTAMPTZ AS timestamp,
    (forecast->>'timestamp')::TIMESTAMPTZ AS forecast_date,
    forecast->>'day' AS day_of_week,
    (forecast->'temperature'->>'low')::FLOAT AS temperature_low,
    (forecast->'temperature'->>'high')::FLOAT AS temperature_high,
    (forecast->'relative_humidity'->>'low')::FLOAT AS humidity_low,
    (forecast->'relative_humidity'->>'high')::FLOAT AS humidity_high,
    forecast->'wind'->>'direction' AS wind_direction,
    (forecast->'wind'->'speed'->>'low')::FLOAT AS wind_speed_low,
    (forecast->'wind'->'speed'->>'high')::FLOAT AS wind_speed_high,
    forecast->'forecast'->>'text' AS forecast_text,
    forecast->'forecast'->>'code' AS forecast_code
FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'records') AS record,
    jsonb_array_elements(record->'forecasts') AS forecast
WHERE 
    r.parameter = 'four-day-forecast'
    AND DATE(r.data_timestamp) = $1
ON CONFLICT (timestamp, forecast_date) DO UPDATE SET
    day_of_week = EXCLUDED.day_of_week,
    temperature_low = EXCLUDED.temperature_low,
    temperature_high = EXCLUDED.temperature_high,
    humidity_low = EXCLUDED.humidity_low,
    humidity_high = EXCLUDED.humidity_high,
    wind_direction = EXCLUDED.wind_direction,
    wind_speed_low = EXCLUDED.wind_speed_low,
    wind_speed_high = EXCLUDED.wind_speed_high,
    forecast_text = EXCLUDED.forecast_text,
    forecast_code = EXCLUDED.forecast_code;

