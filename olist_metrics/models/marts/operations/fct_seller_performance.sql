/*
    Seller performance metrics aggregated by seller and month.

    Note on delivery and review metrics:
    Delivery times and review scores in Olist are captured at order level, not seller level.
    For orders with multiple sellers, it is impossible to attribute delays or reviews to a
    specific seller. Therefore, delivery and review metrics are only computed for single-seller
    orders (seller_count = 1) where attribution is unambiguous. These metrics should be
    interpreted as approximations and treated with caution in analysis.
*/

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

sellers as (
    select * from {{ ref('stg_sellers') }}
),

seller_metrics as (
    select
        order_items.seller_id,
        sellers.seller_city,
        sellers.seller_state,

        -- time dimensions
        date_trunc(date(orders.order_purchase_timestamp), month)            as order_month,
        extract(year from orders.order_purchase_timestamp)                  as order_year,

        -- volume metrics
        count(distinct orders.order_id)                                     as order_count,
        count(order_items.order_item_id)                                    as item_count,

        -- revenue metrics
        round(sum(cast(order_items.price as numeric)), 2)                   as total_revenue,
        round(avg(cast(order_items.price as numeric)), 2)                   as avg_item_price,
        round(sum(cast(order_items.shipping_value as numeric)), 2)          as total_shipping_value,

        -- delivery metrics (single-seller orders only)
        round(avg(case when orders.seller_count = 1 then orders.actual_delivery_days end), 2)   as avg_delivery_days,
        round(avg(case when orders.seller_count = 1 then orders.delivery_delay_days end), 2)    as avg_delay_days,
        countif(orders.seller_count = 1 and orders.is_late_delivery)                            as late_delivery_count,
        round(
            countif(orders.seller_count = 1 and orders.is_late_delivery) /
            nullif(countif(orders.seller_count = 1), 0)
        , 4)                                                                                     as late_delivery_rate,

        -- review metrics (single-seller orders only)
        round(avg(case when orders.seller_count = 1 then orders.avg_review_score end), 2)       as avg_review_score,
        countif(orders.seller_count = 1 and orders.avg_review_score >= 4)                       as positive_review_count,
        countif(orders.seller_count = 1 and orders.avg_review_score <= 2)                       as negative_review_count

    from order_items
    inner join orders
        using (order_id)
    inner join sellers
        using (seller_id)
    where orders.order_status != 'canceled'
    group by
        order_items.seller_id,
        sellers.seller_city,
        sellers.seller_state,
        order_month,
        order_year
)

select * from seller_metrics