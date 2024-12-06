select 
    c.ID as CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    r.ID as ORDER_ID,
    r.ORDER_DATE,
    r.STATUS
from
    {{ ref('raw_customers') }} as c
left join
    {{ ref('raw_orders') }} as r
on c.ID = r.USER_ID