with customers as (
    select * from {{ ref('int_customers_with_orders') }}
),

max_order_date as (
    select date(max(last_order_date)) as reference_date -- this is to keep rfm segmentation realistic as the orders data is old
    from customers 
),

rfm as (
    select 
        customers.customer_id,
        date_diff(
                max_order_date.reference_date,
                date(customers.last_order_date),
                day
        )                                                   as recency_days,
        customers.order_count                               as frequency,
        customers.total_spend                               as monetary,
        ntile(4) over (order by date_diff(
                max_order_date.reference_date,
                date(customers.last_order_date),
                day
        ) asc )                                             as recency_score,
        ntile(4) over (order by customers.order_count asc)  as frequency_score,
        ntile(4) over (order by customers.total_spend asc)  as monetary_score
    from customers 
    cross join max_order_date
),

final as (
    select 
        customers.customer_id,
        customers.order_count,
        customers.first_order_date,
        customers.last_order_date,
        customers.total_spend,
        customers.avg_order_value,
        customers.total_items_purchased,
        customers.avg_review_score,
        customers.is_repeat_customer,
        rfm.recency_days,
        rfm.frequency,
        rfm.monetary,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        rfm.recency_score + rfm.frequency_score + rfm.monetary_score        as rfm_total_score
    from customers 
    left join rfm using (customer_id)
)

select * from final