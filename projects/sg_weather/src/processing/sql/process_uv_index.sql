with
    unique_uv as (
        select distinct
            (record -> 'index' -> 0 ->> 'hour')::timestamptz as timestamp,
            'uv_index' as parameter,
            (record -> 'index' -> 0 ->> 'value')::float as value,
            'index' as units
        from
            raw_weather_data r,
            jsonb_array_elements(r.validated_data -> 'records') as record
        where r.parameter = 'uv-index' and date(r.data_timestamp) = $1
    )
    insert into weather_transformed.national_measurements(
        timestamp, parameter, value, units
    )
select *
from unique_uv on conflict(timestamp, parameter) do update set value = excluded.value
;

