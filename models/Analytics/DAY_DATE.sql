with distinct_dates as (
    select distinct split(DATE,'/')[2]||split(DATE,'/')[1]||split(DATE,'/')[0] as day_id
            , to_date(DATE, 'DD/MM/YYYY') as day_date
            , AVGTEMP
    from {{ ref('hourly_format')}}
    order by day_date),

with day_date as (
    select
        day_id
        , day_date
        , AVGTEMP
    from distinct_dates

)

select * from day_date;