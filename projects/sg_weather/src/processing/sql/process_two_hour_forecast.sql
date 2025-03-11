INSERT INTO weather_transformed.two_hour_forecasts (
    timestamp, valid_from, valid_to, area, forecast_text, forecast_code
)
SELECT
    (item->>'timestamp')::TIMESTAMPTZ as timestamp,
    (item->'valid_period'->>'start')::TIMESTAMPTZ as valid_from,
    (item->'valid_period'->>'end')::TIMESTAMPTZ as valid_to,
    forecast->>'area' as area,
    forecast->>'forecast' as forecast_text,
    NULL as forecast_code
FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'items') as item,
    jsonb_array_elements(item->'forecasts') as forecast
WHERE 
    r.parameter = 'two-hour-forecast'
    AND DATE(r.data_timestamp) = $1
ON CONFLICT (timestamp, area) DO UPDATE SET
    valid_from = EXCLUDED.valid_from,
    valid_to = EXCLUDED.valid_to,
    forecast_text = EXCLUDED.forecast_text,
    forecast_code = EXCLUDED.forecast_code;

