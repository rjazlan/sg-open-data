-- Extract stations data from raw_weather_data
with
    area_data as (
        select distinct
            on (area ->> 'name')
            area ->> 'name' as name,
            (area -> 'location' ->> 'latitude')::float as latitude,
            (area -> 'location' ->> 'longitude')::float as longitude
        from
            raw_weather_data,
            jsonb_array_elements(validated_data -> 'area_metadata') as area
        where parameter in ('two-hour-forecast')
    )
    insert into weather_transformed.areas(name, latitude, longitude)
select *
from
    area_data on conflict(name) do update set
    name = excluded.name,
    latitude = excluded.latitude,
    longitude = excluded.longitude
;

