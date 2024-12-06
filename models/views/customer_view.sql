{{
    config(
        materialized='view'
    )
}}

select ID, FIRST_NAME from {{ source('sales', 'customers') }}