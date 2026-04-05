/* 
    Order-level feature store for ML models (delivery delay prediction).
    Grain: one row per order.
    Target variable: delivery_delay_days or is_late_delivery 
*/

with orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

item_features as (
    select 
        order_items.order_id,
        count(order_items.order_item_id)                                            as item_count,
        count(distinct order_items.seller_id)                                       as seller_count,
        count(distinct products.product_category_name_english)                      as category_count,
        round(sum(cast(order_items.price as numeric)), 2)                           as total_items_price,
        round(sum(cast(order_items.price as numeric)), 2)                           as avg_item_price,
        round(sum(cast(order_items.shipping_value as numeric)), 2)                  as total_shipping_value,
        round(avg(products.product_weight_g), 2)                                    as avg_product_weight_g,
        round(avg(products.product_volume_cm3), 2)                                  as avg_product_volume_cm3
    from order_items 
    inner join products 
        using (product_id)
    group by order_id
),

final as (
    select 
        -- identifiers 
        orders.order_id,
        orders.customer_id,

        -- metadata 
        orders.order_purchase_timestamp                                             as meta_order_purchase_timestamp,
        orders.order_status,

        -- customer location features 
        orders.customer_state,

        -- time features 
        extract(hour from orders.order_purchase_timestamp)                          as order_hour,
        extract(dayofweek from orders.order_purchase_timestamp)                     as order_day_of_week,
        extract(month from orders.order_purchase_timestamp)                         as order_month,
        extract(year from orders.order_purchase_timestamp)                          as order_year,

        -- item features 
        item_features.item_count,
        item_features.seller_count,
        item_features.category_count,
        item_features.total_items_price,
        item_features.avg_item_price,
        item_features.total_shipping_value,
        item_features.avg_product_weight_g,
        item_features.avg_product_volume_cm3,

        -- payment features 
        orders.total_payment_value,
        orders.max_payment_installments,
        orders.payment_type_count,
        orders.has_credit_card,
        orders.has_boleto,
        orders.has_voucher,
        orders.has_debit_card,

        -- target variables 
        orders.actual_delivery_days,
        orders.estimated_delivery_days,
        orders.delivery_delay_days,
        orders.is_late_delivery

    from orders 
    left join item_features
        using (order_id)
    where orders.order_status = 'delivered'
)

select * from final
