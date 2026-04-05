/*
    
    Customer-level feature store for ML models (churn, LTV, segmentation).
    Grain: one row per customer.
    All features are computed as of the customer's last order date to avoid data leakage.

*/

with customer_orders as (
    select * from {{ ref('fct_customer_orders') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_order_stats as (
    select 
        customer_orders.customer_id,

        -- recency features 
        customer_orders.recency_days,
        customer_orders.first_order_date,
        customer_orders.last_order_date,
        date_diff(
            date(customer_orders.last_order_date),
            date(customer_orders.first_order_date),
            day
        )                                                                           as customer_lifespan_days,

        -- frequency features
        customer_orders.order_count,
        customer_orders.total_items_purchased,

        -- monetary features 
        customer_orders.total_spend,
        customer_orders.avg_order_value,

        -- rfm features
        customer_orders.recency_score,
        customer_orders.frequency_score,
        customer_orders.monetary_score,
        customer_orders.rfm_total_score,

        -- behavioral features 
        customer_orders.avg_review_score,
        customer_orders.is_repeat_customer,

        -- order pattern features 
        round(
            customer_orders.total_spend / nullif(customer_orders.order_count, 0)
        , 2)                                                                       as avg_spend_per_order,
    
        round(
            customer_orders.total_items_purchased / nullif(customer_orders.order_count, 0)
        , 2)                                                                       as avg_items_per_order

        from customer_orders
),

payment_features as (
    select 
        customer_id,
        countif(has_credit_card)                                                    as credit_card_order_count,
        countif(has_boleto)                                                         as boleto_order_count,
        countif(has_voucher)                                                        as voucher_order_count,
        round(avg(max_payment_installments), 2)                                     as avg_payment_installments,
    from orders 
    group by customer_id
),

delivery_features as (
    select 
        customer_id,
        round(avg(actual_delivery_days), 2)                                         as avg_actual_delivery_days,
        round(avg(delivery_delay_days), 2)                                          as avg_delivery_delay_days,
        countif(is_late_delivery)                                                   as late_delivery_count,
        round(countif(is_late_delivery) / nullif(count(*), 0), 4)                   as late_delivery_rate
    from orders 
    group by customer_id
),

final as (
    select 
        -- identifiers 
        customer_order_stats.customer_id,
        dim_customers.customer_state,
        dim_customers.customer_segment,

        -- recency features 
        customer_order_stats.recency_days,
        customer_order_stats.customer_lifespan_days,
        customer_order_stats.first_order_date                                       as meta_first_order_date,
        customer_order_stats.last_order_date                                        as meta_last_order_date,

        -- frequency features 
        customer_order_stats.order_count,
        customer_order_stats.total_items_purchased,
        customer_order_stats.avg_items_per_order,

        -- monetary features 
        customer_order_stats.total_spend,
        customer_order_stats.avg_order_value,
        customer_order_stats.avg_spend_per_order,

        -- rfm features 
        customer_order_stats.recency_score,
        customer_order_stats.frequency_score,
        customer_order_stats.monetary_score,
        customer_order_stats.rfm_total_score,

        -- behavioral features 
        customer_order_stats.avg_review_score,
        customer_order_stats.is_repeat_customer,

        -- payment features 
        payment_features.credit_card_order_count,
        payment_features.boleto_order_count,
        payment_features.voucher_order_count,
        payment_features.avg_payment_installments,

        -- delivery features 
        delivery_features.avg_actual_delivery_days,
        delivery_features.avg_delivery_delay_days,
        delivery_features.late_delivery_count,
        delivery_features.late_delivery_rate

    from customer_order_stats
    left join dim_customers
        using (customer_id)
    left join payment_features
        using (customer_id)
    left join delivery_features 
        using (customer_id)
)

select * from final 

