select
    cast(Date as date) as market_date,
    cast(Close as {{ type_float() }}) as yield_10y
from {{ source('bronze', 'yield_10y') }}
