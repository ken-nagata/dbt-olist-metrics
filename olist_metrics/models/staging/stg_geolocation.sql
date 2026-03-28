with source as (
    select * from {{ source('olist_raw','geolocation')}}
),
renamed as (
    select 
        cast(geolocation_zip_code_prefix as string) as geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        lower(geolocation_city)                     as geolocation_city,
        lower(geolocation_state)                    as geolocation_state
    from source
)
select * from renamed