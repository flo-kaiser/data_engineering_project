{{ config(materialized='table') }}

with raw as (
    select * from {{ source('bronze', 'gold_prices') }}
),

cleaned as (
    select
        cast(col_2 as date) as price_date,
        cast(nullif(col_3, 'nan') as double) as usd,
        cast(nullif(col_4, 'nan') as double) as eur,
        cast(nullif(col_5, 'nan') as double) as jpy,
        cast(nullif(col_6, 'nan') as double) as gbp,
        cast(nullif(col_7, 'nan') as double) as cad,
        cast(nullif(col_8, 'nan') as double) as chf,
        cast(nullif(col_9, 'nan') as double) as inr,
        cast(nullif(col_10, 'nan') as double) as cny,
        cast(nullif(col_11, 'nan') as double) as "try",
        cast(nullif(col_12, 'nan') as double) as sar,
        cast(nullif(col_13, 'nan') as double) as idr,
        cast(nullif(col_14, 'nan') as double) as aed,
        cast(nullif(col_15, 'nan') as double) as thb,
        cast(nullif(col_16, 'nan') as double) as vnd,
        cast(nullif(col_17, 'nan') as double) as egp,
        cast(nullif(col_18, 'nan') as double) as krw,
        cast(nullif(col_19, 'nan') as double) as rub,
        cast(nullif(col_20, 'nan') as double) as zar,
        cast(nullif(col_21, 'nan') as double) as aud
    from raw
    where col_2 != 'nan' 
      and col_2 != 'NaT'
      and col_2 like '____-__-__%'
),

melted as (
    unpivot cleaned
    on usd, eur, jpy, gbp, cad, chf, inr, cny, "try", sar, idr, aed, thb, vnd, egp, krw, rub, zar, aud
    into
        name currency
        value price
)

select 
    price_date,
    upper(currency) as currency,
    price
from melted
