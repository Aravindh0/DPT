with customers as (
    select 
        CUSTOMER_ID,
        ORDER_ID,
        FIRST_NAME,
        LAST_NAME,
        STATUS,
        min(ORDER_DATE) as FIRST_ORDER_DATE,
        max(ORDER_DATE) as RECENT_ORDER_DATE
    from
        {{ ref('src_customer') }}
    group by CUSTOMER_ID, ORDER_ID, FIRST_NAME, LAST_NAME, STATUS
),
payment as (
    select 
        ORDERID as ORDER_ID,
        PAYMENTMETHOD,
        STATUS,
        sum(AMOUNT) as TOTAL_PURCHASE_AMOUNT
    from
        {{ ref('raw_payment') }}
    group by ORDER_ID, PAYMENTMETHOD, STATUS
),
final as (
    select
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        count(c.ORDER_ID) as NO_OF_ORDERS,
        c.FIRST_ORDER_DATE,
        c.RECENT_ORDER_DATE,
        sum(p.TOTAL_PURCHASE_AMOUNT) as TOTAL_PURCHASE_AMOUNT,
        c.STATUS
    from
        customers as c
    inner join
        payment as p
    on p.ORDER_ID = c.ORDER_ID
    where 
        c.STATUS = 'completed' 
        and p.STATUS = 'success'
    group by 
        c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.FIRST_ORDER_DATE, c.RECENT_ORDER_DATE, c.STATUS
)
select 
    CUSTOMER_ID, 
    FIRST_NAME, 
    NO_OF_ORDERS, 
    FIRST_ORDER_DATE, 
    RECENT_ORDER_DATE, 
    TOTAL_PURCHASE_AMOUNT
from 
    final
