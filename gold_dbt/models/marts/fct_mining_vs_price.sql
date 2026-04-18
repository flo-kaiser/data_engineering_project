/*
    MODEL: fct_mining_vs_price
    DESCRIPTION: 
    Gold Layer - Yearly comparison between global gold mining production volumes 
    and average market prices.
*/

{{ config(materialized='table') }}

with yearly_prices as (
    select
        date_part('year', price_date) as market_year,
        avg(price_usd_per_oz) as avg_gold_price_usd
    from {{ ref('stg_gold_prices') }}
    group by 1
),

mining as (
    select
        production_year as market_year,
        sum(production_tonnes) as total_mining_production_tonnes
    from {{ ref('stg_mining_production') }}
    group by 1
)

select
    m.market_year,
    m.total_mining_production_tonnes,
    p.avg_gold_price_usd
from mining m
join yearly_prices p on m.market_year = p.market_year
order by 1 desc
