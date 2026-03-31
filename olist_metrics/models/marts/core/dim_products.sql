with products as (
    select * from {{ ref('int_products_with_category') }}
),

final as (
    select * from products
)

select * from final