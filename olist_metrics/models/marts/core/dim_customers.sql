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
        ntile(4) over (order by date_diff(max_order_date.reference_date,
                                            date(customers.last_order_date),
                                            day
        ) asc)                                                                       as recency_score,
        ntile(4) over (order by customers.order_count asc)                           as frequency_score,
        ntile(4) over (order by customers.total_spend asc)                           as monetary_score
    from customers 
    cross join max_order_date
),

final as (
    select 
        customers.customer_id,
        customers.customer_city,
        customers.customer_state,
        customers.customer_zip_code_prefix,
        case
            when rfm.recency_score = 4 and rfm.frequency_score = 4 and rfm.monetary_score = 4 then 'champions'
            when rfm.frequency_score >= 3 and rfm.monetary_score >= 3 then 'loyal'
            when rfm.recency_score >= 3 and rfm.frequency_score >= 2 then 'potential_loyalist'
            when rfm.recency_score >= 3 and rfm.frequency_score = 1 and rfm.monetary_score <= 2 then 'promising'
            when rfm.recency_score >= 3 and rfm.frequency_score = 1 then 'new_customer'
            when rfm.recency_score = 2 and rfm.frequency_score >= 2 and rfm.monetary_score >= 2 then 'needs_attention'
            when rfm.recency_score = 1 and rfm.frequency_score >= 3 and rfm.monetary_score >= 3 then 'cant_lose_them'
            when rfm.recency_score <= 2 and rfm.frequency_score >= 3 and rfm.monetary_score >= 3 then 'at_risk'
            when rfm.recency_score <= 2 and rfm.frequency_score >= 3 then 'frequent_low_spender'
            when rfm.recency_score = 1 and rfm.frequency_score = 1 and rfm.monetary_score = 1 then 'lost'
            when rfm.recency_score <= 2 and rfm.frequency_score <= 2 then 'hibernating'
            else 'other'
        end as customer_segment
    from customers 
    left join rfm using (customer_id)
)           

select * from final