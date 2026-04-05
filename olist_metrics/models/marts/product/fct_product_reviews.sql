/*
    Product-level review metrics.
    
    Note on attribution:
    Reviews in Olist are captured at order level, not product level. For orders with
    multiple products, it is impossible to attribute a review to a specific product.
    Therefore, this table only includes single-product orders (item_count = 1) where
    attribution is unambiguous. These metrics should be interpreted as approximations
    and treated with caution in analysis.
*/

with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

product_review_metrics as (
    select
        -- product and category context
        products.product_id,
        products.product_category_name_english,

        -- time dimensions
        date_trunc(date(orders.order_purchase_timestamp), month)                                    as order_month,
        extract(year from orders.order_purchase_timestamp)                                          as order_year,

        -- review metrics
        count(reviews.review_id)                                                                    as review_count,
        round(avg(reviews.review_score), 2)                                                         as avg_review_score,
        countif(reviews.review_score = 5)                                                           as five_star_count,
        countif(reviews.review_score = 4)                                                           as four_star_count,
        countif(reviews.review_score = 3)                                                           as three_star_count,
        countif(reviews.review_score = 2)                                                           as two_star_count,
        countif(reviews.review_score = 1)                                                           as one_star_count,
        countif(reviews.review_score >= 4)                                                          as positive_review_count,
        countif(reviews.review_score <= 2)                                                          as negative_review_count,
        round(countif(reviews.review_score >= 4) / nullif(count(reviews.review_id), 0), 4)          as positive_review_rate

    from reviews
    inner join orders
        using (order_id)
    inner join order_items
        using (order_id)
    inner join products
        using (product_id)
    where orders.order_status = 'delivered' -- reviews belonging only to delivered orders 
        and orders.item_count = 1
    group by 1, 2, 3, 4
)

select * from product_review_metrics