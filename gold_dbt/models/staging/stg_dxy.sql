select
    cast(Date as date) as market_date,
    cast(Close as {{ type_float() }}) as dxy_close
from {{ source('bronze', 'dxy') }}
