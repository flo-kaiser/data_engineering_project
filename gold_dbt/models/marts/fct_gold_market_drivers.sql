/*
    MODEL: fct_gold_market_drivers
    DESCRIPTION: 
    Gold Layer - Aggregates various market drivers (ETF Flows, DXY, Yields) 
    against Gold Prices for monthly correlation analysis.
*/

{{ config(materialized='table') }}

with prices as (
    select
        date_trunc('month', price_date) as market_month,
        avg(price_usd_per_oz) as gold_price_usd
    from {{ ref('stg_gold_prices') }}
    group by 1
),

etf as (
    select
        date_trunc('month', flow_date) as market_month,
        sum(flow_usd_mn) as total_etf_flow_usd_mn
    from {{ ref('stg_etf_flows') }}
    group by 1
),

dxy as (
    select
        date_trunc('month', market_date) as market_month,
        avg(dxy_close) as avg_dxy
    from {{ ref('stg_dxy') }}
    group by 1
),

yields as (
    select
        date_trunc('month', market_date) as market_month,
        avg(yield_10y) as avg_yield_10y
    from {{ ref('stg_yield_10y') }}
    group by 1
)

select
    p.market_month,
    p.gold_price_usd,
    e.total_etf_flow_usd_mn,
    d.avg_dxy,
    y.avg_yield_10y
from prices p
left join etf e on p.market_month = e.market_month
left join dxy d on p.market_month = d.market_month
left join yields y on p.market_month = y.market_month
order by 1 desc
