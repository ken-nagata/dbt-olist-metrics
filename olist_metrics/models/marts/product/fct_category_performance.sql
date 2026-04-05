with order_items as (
    select * from  {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

category_metrics as (
    select 
        products.product_category_name_english,
        date_trunc(date(orders.order_purchase_timestamp), month)                                            as order_month,
        extract(year from orders.order_purchase_timestamp)                                                  as order_year,

        -- volume metrics
        count(distinct orders.order_id)                                                                     as order_count,
        count(order_items.order_item_id)                                                                    as item_count,
        count(distinct order_items.seller_id)                                                               as seller_count,
        count(distinct orders.customer_id)                                                                  as customer_count,

        -- revenue metrics 
        round(sum(cast(order_items.price as numeric)), 2)                                                   as total_revenue,
        round(avg(cast(order_items.price as numeric)), 2)                                                   as avg_item_price,
        round(sum(cast(order_items.shipping_value as numeric)), 2)                                          as total_shipping_value,

        -- delivery metrics 
        round(avg(orders.actual_delivery_days), 2)                                                          as avg_delivery_days,
        round(avg(orders.delivery_delay_days), 2)                                                           as avg_delay_days,
        countif(orders.is_late_delivery)                                                                    as late_delivery_count, 
        round(countif(orders.is_late_delivery) / nullif(count(distinct orders.order_id), 0), 4)             as late_delivery_rate,

        -- review metrics 
        round(avg(orders.avg_review_score), 2)                                                              as avg_review_score
    
    from order_items 
    inner join orders 
        using (order_id)
    inner join products 
        using (product_id)
    where orders.order_status != 'canceled'
    group by 1, 2, 3
)

select * from category_metrics