INSERT INTO weather_transformed.weather_measurements (timestamp, station_id, parameter, value, units)
WITH readings_data AS (
  SELECT
    (reading->>'timestamp')::TIMESTAMPTZ as timestamp,
    reading_data->>'stationId' as station_id,
    r.parameter,
    (reading_data->>'value')::FLOAT as value,
    r.validated_data->>'reading_unit' as units
  FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'readings') as reading,
    jsonb_array_elements(reading->'data') as reading_data
  WHERE 
    r.parameter IN ('temperature', 'rainfall', 'humidity', 'wind-speed', 'wind-direction')
    AND DATE(r.data_timestamp) = $1
)
SELECT * FROM readings_data
ON CONFLICT (timestamp, station_id, parameter) DO UPDATE SET
  value = EXCLUDED.value

