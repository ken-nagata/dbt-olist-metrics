with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

reviews_aggregated as (
    select 
        order_id,
        count(review_id)                                as review_count,
        round(cast(avg(review_score) as float64), 2)    as avg_review_score,
        min(review_score)                               as min_review_score,
        max(review_score)                               as max_review_score,
        min(review_creation_date)                       as first_review_date,
        max(review_answer_timestamp)                    as last_review_answer_timestamp
    from reviews
    group by order_id
)

select * from reviews_aggregated