{{
    config(
        materialized='incremental',
        unique_key=['DAY_ID', 'HR_PERIOD'],
        incremental_strategy='append'
    )

}}

with consumption as (
    select distinct split(DATE,'/')[2]||split(DATE,'/')[1]||split(DATE,'/')[0] as DAY_ID
        , to_date(DATE, 'DD/MM/YYYY') as DAY_DATE
        , TIMEPERIOD AS HR_PERIOD
        , RATETYPE AS RATE 
        , CONSUMPTIONKWH AS KWH
        , COST
        , TO_NUMBER(SUBSTR(COST,2), 10, 2) AS HR_COST
        , current_timestamp as ETL_LOAD_TS
    from {{ ref('hourly_format')}}

)

select * from consumption
{% if is_incremental() %}
    where DAY_ID > (select max(DAY_ID) from {{this}})
{% endif %}