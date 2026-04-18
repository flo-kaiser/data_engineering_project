with raw as (
    select * from {{ source('bronze', 'gdt_demand') }}
),

quarters as (
    select * from raw where col_22 = 'Q1''10'
),

quarter_mapping as (
    unpivot quarters
    on columns(* exclude (col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, col_86, col_87, source_file, ingested_at))
    into name col_name value q_label
),

data_rows as (
    select * from raw 
    where col_1 is not null 
      and col_1 not in ('Gold supply and demand WGC presentation', 'nan')
      and col_22 != 'Q1''10'
),

melted_data as (
    unpivot data_rows
    on columns(* exclude (col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, col_86, col_87, source_file, ingested_at))
    into name col_name value tonnes
)

select
    case 
        when q.q_label like 'Q1%' then '20' || substr(q.q_label, 4, 2) || '-03-31'
        when q.q_label like 'Q2%' then '20' || substr(q.q_label, 4, 2) || '-06-30'
        when q.q_label like 'Q3%' then '20' || substr(q.q_label, 4, 2) || '-09-30'
        when q.q_label like 'Q4%' then '20' || substr(q.q_label, 4, 2) || '-12-31'
    end::date as quarter_date,
    d.col_1 as demand_component,
    cast(nullif(d.tonnes, 'nan') as double) as tonnes
from melted_data d
join quarter_mapping q on d.col_name = q.col_name
where tonnes is not null
