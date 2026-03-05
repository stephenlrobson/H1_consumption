{{
    config(
        materialized='incremental',
        unique_key='day_id'
    )

}}

with distinct_dates as (
    select distinct split(DATE,'/')[2]||split(DATE,'/')[1]||split(DATE,'/')[0] as day_id
            , to_date(DATE, 'DD/MM/YYYY') as day_date
            , AVGTEMP
            , current_timestamp as ETL_LOAD_TS
    from {{ ref('hourly_format')}}
    order by day_date),

day_date as (
    select
        day_id
        , day_date
        , AVGTEMP
        , ETL_LOAD_TS
    from distinct_dates

)

select * from day_date
{% if is_incremental() %}
    where (day_id > (select max(day_id) from {{ this }}))
        OR 
        (day_id = (select day_id from {{ this }} where AVGTEMP IS NULL))
{% endif %}