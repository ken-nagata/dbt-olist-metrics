/* 
    CAC Payback analysis using mock marketing spend data. Marketing spend is synthetic - created for portfolio demonstration purposes only.
    
    CAC = total marketing spend / number of new customers acquired in that month

    Payback period = CAC / average order value of new customers

*/

with customer_orders as (
    select * from {{ ref('fct_customer_orders') }}

),

orders as (
    select * from {{ ref('fct_orders') }}
),

marketing_spend_raw as (
    select * from {{ ref('marketing_spend') }}
),

new_customers as (
    select
        date_trunc(date(customer_orders.first_order_date), month)            as order_month,
        count(customer_orders.customer_id)                                   as new_customer_count,
        round(avg(orders.total_payment_value), 2)                            as avg_first_order_value
    from customer_orders
    inner join orders
        on customer_orders.customer_id = orders.customer_id
        and date(orders.order_purchase_timestamp) = date(customer_orders.first_order_date)
    group by 1
),

marketing_spend as (
    select 
        parse_date('%Y-%m', month)                                           as order_month,
        sum(spend_brl)                                                       as total_spend_brl
    from marketing_spend_raw
    group by 1 
),

final as (
    select 
        new_customers.order_month,
        new_customers.new_customer_count,
        new_customers.avg_first_order_value,
        marketing_spend.total_spend_brl,
        round(
            marketing_spend.total_spend_brl / nullif(new_customers.new_customer_count, 0) 
        ,2)                                                                                       as cac_brl,
        round(
            (marketing_spend.total_spend_brl / nullif(new_customers.new_customer_count, 0))
            / nullif(new_customers.avg_first_order_value, 0)
        ,2)                                                                                       as cac_payback_months
    from new_customers
    left join marketing_spend
        using (order_month)
)

select * from final