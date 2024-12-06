select * from {{ ref('raw_payment') }}
where {{ markup1('AMOUNT')}}