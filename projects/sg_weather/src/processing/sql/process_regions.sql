-- Extract region metadata from PM25 and PSI data
WITH region_data AS (
    SELECT DISTINCT 
        region->>'name' as region_id,
        (region->'location'->>'latitude')::FLOAT as latitude,
        (region->'location'->>'longitude')::FLOAT as longitude
    FROM 
        raw_weather_data,
        jsonb_array_elements(validated_data->'region_metadata') as region
    WHERE 
        parameter IN ('pm25', 'psi')
)
INSERT INTO weather_transformed.regions (region_id, latitude, longitude)
SELECT * FROM region_data
ON CONFLICT (region_id) DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;
