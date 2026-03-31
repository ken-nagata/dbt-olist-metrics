with products as (
    select * from {{ ref('stg_products') }}
),
category_translations as (
    select * from {{ ref('stg_product_category_name_translation') }}
),
final as (
    select 
        products.product_id,
        products.product_category_name,
        category_translations.product_category_name_english,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,
        products.product_length_cm * products.product_height_cm * products.product_width_cm   as product_volume_cm3
    from products 
    left join category_translations
        using (product_category_name)
)

select * from final