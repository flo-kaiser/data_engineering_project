select
    cast(Date as date) as market_date,
    cast(Close as {{ type_float() }}) as sp500_close
from {{ source('bronze', 'sp500') }}
