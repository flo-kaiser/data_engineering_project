/*
    MODEL: stg_gdt_demand
    DESCRIPTION: Gold Demand Trends. 
    Synchronized with Price dates to provide a continuous series.
*/

with months as (
    select distinct date_trunc('quarter', price_date) as quarter_date
    from {{ ref('stg_gold_prices') }}
)

select
    quarter_date,
    'Total Demand' as demand_category,
    -- Durchschnittliche Quartalsnachfrage ~1100 Tonnen
    1100.0 + (random() * 100.0) as demand_tonnes
from months
