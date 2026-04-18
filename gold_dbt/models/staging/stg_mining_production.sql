with raw as (
    select * from {{ source('bronze', 'mining_production') }}
),

years as (
    select * from raw where col_1 = '2010.0'
),

year_mapping as (
    unpivot years
    on columns(* exclude (col_0, source_file, ingested_at))
    into name col_name value production_year
),

data_rows as (
    select * from raw 
    where col_0 is not null 
      and col_0 not in ('Gold Mine Production (tonnes)', 'Source: Metals Focus', 'nan')
      and col_1 != '2010.0'
),

melted_data as (
    unpivot data_rows
    on columns(* exclude (col_0, source_file, ingested_at))
    into name col_name value production_tonnes
)

select
    cast(floor(cast(y.production_year as double)) as integer) as production_year,
    d.col_0 as region_or_country,
    cast(nullif(d.production_tonnes, 'nan') as double) as production_tonnes
from melted_data d
join year_mapping y on d.col_name = y.col_name
where production_tonnes is not null
