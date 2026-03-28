with source as (
    select * from {{ source('olist_raw','order_reviews') }}
),
renamed as (
    select 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        datetime(review_creation_date, 'America/Sao_Paulo')         as review_creation_date,
        datetime(review_answer_timestamp, 'America/Sao_Paulo')      as review_answer_timestamp
    from source
)
select * from renamed