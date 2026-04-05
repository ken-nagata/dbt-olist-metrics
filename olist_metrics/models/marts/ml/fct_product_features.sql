/*
    Product-level feature store for ML models (recommendation, price optimization).
    Grain: one row per product.
*/

with products as (
    select * from {{ ref('dim_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

product_reviews as (
    select * from {{ ref('fct_product_reviews') }}
),

sales_features as (
    select
        order_items.product_id,
        count(distinct orders.order_id)                             as total_orders,
        count(order_items.order_item_id)                            as total_items_sold,
        count(distinct orders.customer_id)                          as unique_customers,
        count(distinct order_items.seller_id)                       as seller_count,
        round(sum(cast(order_items.price as numeric)), 2)           as total_revenue,
        round(avg(cast(order_items.price as numeric)), 2)           as avg_selling_price,
        round(min(cast(order_items.price as numeric)), 2)           as min_selling_price,
        round(max(cast(order_items.price as numeric)), 2)           as max_selling_price,
        round(avg(cast(order_items.shipping_value as numeric)), 2)  as avg_shipping_value
    from order_items
    inner join orders
        using (order_id)
    where orders.order_status != 'canceled'
    group by product_id
),

review_features as (
    select
        product_id,
        sum(review_count)                                           as total_reviews,
        round(avg(avg_review_score), 2)                             as avg_review_score,
        sum(positive_review_count)                                  as positive_review_count,
        sum(negative_review_count)                                  as negative_review_count,
        round(avg(positive_review_rate), 4)                         as avg_positive_review_rate
    from product_reviews
    group by product_id
),

final as (
    select
        -- identifiers
        products.product_id,
        products.product_category_name_english,

        -- physical features
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,
        products.product_volume_cm3,

        -- content features
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,

        -- sales features
        sales_features.total_orders,
        sales_features.total_items_sold,
        sales_features.unique_customers,
        sales_features.seller_count,
        sales_features.total_revenue,
        sales_features.avg_selling_price,
        sales_features.min_selling_price,
        sales_features.max_selling_price,
        sales_features.avg_shipping_value,

        -- review features
        coalesce(review_features.total_reviews, 0)                  as total_reviews,
        review_features.avg_review_score,
        coalesce(review_features.positive_review_count, 0)          as positive_review_count,
        coalesce(review_features.negative_review_count, 0)          as negative_review_count,
        review_features.avg_positive_review_rate

    from products
    left join sales_features
        using (product_id)
    left join review_features
        using (product_id)
)

select * from final