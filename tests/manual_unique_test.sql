select id, count(*)as ct from {{ ref('raw_orders') }}
group by id
having ct > 1