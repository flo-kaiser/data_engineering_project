/*
    MODEL: stg_etf_flows
    DESCRIPTION: Normalizes Gold ETF data using GLD Proxy from Yahoo Finance.
*/

with raw as (
    select * from {{ source('bronze', 'etf_flows_proxy') }}
)

select
    cast(Date as date) as flow_date,
    'GLD' as ticker,
    cast(Volume as {{ type_float() }}) as flow_tonnes, -- Proxy: Volume als Aktivitätsindikator
    cast(Close as {{ type_float() }}) as flow_usd_mn -- Proxy: Preis als Wertindikator
from raw
