select
    Date::date as market_date,
    cast(Close as double) as dxy_close
from {{ source('bronze', 'dxy') }}
