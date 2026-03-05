
with consumption as (
    select DATE
            , TIMEPERIOD
            , RATETYPE
            , CONSUMPTIONKWH
            , COST
            , AVGTEMP
    from {{ source("h1", "HOURLY_FORMAT")}}
    where DATE IS NOT NULL
)

select * from consumption

