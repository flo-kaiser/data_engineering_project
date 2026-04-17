select
    Date::date as market_date,
    cast(Close as double) as sp500_close
from {{ source('bronze', 'sp500') }}
