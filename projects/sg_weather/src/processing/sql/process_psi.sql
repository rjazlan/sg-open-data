-- PSI data (multiple parameters)
INSERT INTO weather_transformed.weather_measurements (timestamp, station_id, parameter, value, units)
SELECT
    (item->>'timestamp')::TIMESTAMPTZ as timestamp,
    regions.region as station_id,
    CASE
        WHEN metric = 'psi_twenty_four_hourly' THEN 'psi_24h'
        WHEN metric = 'pm25_twenty_four_hourly' THEN 'pm25_24h'
        WHEN metric = 'pm10_twenty_four_hourly' THEN 'pm10_24h'
        WHEN metric = 'o3_eight_hour_max' THEN 'o3_8h'
        WHEN metric = 'co_eight_hour_max' THEN 'co_8h'
        WHEN metric = 'co_sub_index' THEN 'co_subindex'
        WHEN metric = 'o3_sub_index' THEN 'o3_subindex'
        WHEN metric = 'pm10_sub_index' THEN 'pm10_subindex'
        WHEN metric = 'pm25_sub_index' THEN 'pm25_subindex'
        WHEN metric = 'so2_sub_index' THEN 'so2_subindex'
        ELSE metric
    END as parameter,
    regions.value::FLOAT as value,
    'index' as units
FROM 
    raw_weather_data r,
    jsonb_array_elements(r.validated_data->'items') as item,
    jsonb_each(item->'readings') as metrics(metric, values),
    jsonb_each_text(metrics.values) as regions(region, value)
WHERE 
    r.parameter = 'psi'
    AND DATE(r.data_timestamp) = $1
ON CONFLICT (timestamp, station_id, parameter) DO UPDATE SET
    value = EXCLUDED.value;

