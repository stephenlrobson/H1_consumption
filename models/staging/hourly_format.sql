
with consumption as (
    select DATE
            , TIMEPERIOD
            , RATETYPE
            , CONSUMPTIONKWH
            , COST
            , AVGTEMP
    from {{ source("h1", "HOURLY_FORMAT")}}
)

select * from consumption

