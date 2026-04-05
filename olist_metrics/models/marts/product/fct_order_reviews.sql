with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

review_metrics as (
    select
        -- time dimensions
        date_trunc(date(orders.order_purchase_timestamp), month)             as order_month,
        extract(year from orders.order_purchase_timestamp)                   as order_year,

        -- geography
        orders.customer_state,

        -- review metrics
        count(reviews.review_id)                                             as review_count,
        round(avg(reviews.review_score), 2)                                  as avg_review_score,
        countif(reviews.review_score = 5)                                    as five_star_count,
        countif(reviews.review_score = 4)                                    as four_star_count,
        countif(reviews.review_score = 3)                                    as three_star_count,
        countif(reviews.review_score = 2)                                    as two_star_count,
        countif(reviews.review_score = 1)                                    as one_star_count,
        countif(reviews.review_score >= 4)                                   as positive_review_count,
        countif(reviews.review_score <= 2)                                   as negative_review_count,
        round(countif(reviews.review_score >= 4) / nullif(count(reviews.review_id), 0), 4) as positive_review_rate

    from reviews
    inner join orders
        using (order_id)
    where orders.order_status = 'delivered' -- reviews belonging only to delivered orders 
    group by 1, 2, 3
)

select * from review_metrics