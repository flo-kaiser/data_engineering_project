select
    Date::date as market_date,
    cast(Close as double) as yield_10y
from {{ source('bronze', 'yield_10y') }}
