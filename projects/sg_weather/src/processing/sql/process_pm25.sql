-- PM2.5 data
INSERT INTO weather_transformed.weather_measurements (timestamp, station_id, parameter, value, units)
SELECT
    (item->>'timestamp')::TIMESTAMPTZ as timestamp,
    regions.region as station_id,
    'pm25' as parameter,
    regions.value::FLOAT as value,
    'mcg/m3' as units
FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'items') as item,
    jsonb_each_text(item->'readings'->'pm25_one_hourly') as regions(region, value)
WHERE 
    r.parameter = 'pm25'
    AND DATE(r.data_timestamp) = $1
ON CONFLICT (timestamp, station_id, parameter) DO UPDATE SET
    value = EXCLUDED.value;

