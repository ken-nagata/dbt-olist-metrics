with source as (
    select * from {{ source('olist_raw','customers')}}
),
renamed as (
    select
        customer_id,
        customer_unique_id,
        cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
        lower(customer_city)                     as customer_city,
        lower(customer_state)                    as customer_state
    from source
)
select * from renamed