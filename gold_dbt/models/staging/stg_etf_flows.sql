with raw as (
    select * from {{ source('bronze', 'etf_flows') }}
),

tickers as (
    select * from raw where col_0 = 'ticker'
),

ticker_mapping as (
    unpivot tickers
    on columns(* exclude (col_0, col_1, col_2, col_3, col_4))
    into 
        name col_name 
        value ticker
),

data_rows as (
    select * from raw 
    where col_0 not in ('ticker', 'nan')
      and col_0 like '____-__-__%'
),

melted_data as (
    unpivot data_rows
    on columns(* exclude (col_0, col_1, col_2, col_3, col_4))
    into 
        name col_name 
        value flow
)

select
    cast(d.col_0 as date) as flow_date,
    t.ticker,
    cast(nullif(d.flow, 'nan') as double) as flow_usd_mn
from melted_data d
join ticker_mapping t on d.col_name = t.col_name
where flow_usd_mn is not null
