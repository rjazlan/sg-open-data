-- Extract stations data from raw_weather_data
WITH station_data AS (
SELECT DISTINCT ON (station->>'id')
    station->>'id' as station_id,
    station->>'name' as name,
    (station->'location'->>'latitude')::FLOAT as latitude,
    (station->'location'->>'longitude')::FLOAT as longitude,
    station->>'device_id' as device_id
FROM 
    raw_weather_data,
    jsonb_array_elements(validated_data->'stations') as station
WHERE 
    parameter IN ('temperature', 'rainfall', 'humidity', 'wind-speed', 'wind-direction')
)
INSERT INTO weather_transformed.stations (station_id, name, latitude, longitude, device_id)
SELECT * FROM station_data
ON CONFLICT (station_id) DO UPDATE SET
    name = EXCLUDED.name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    device_id = EXCLUDED.device_id;
