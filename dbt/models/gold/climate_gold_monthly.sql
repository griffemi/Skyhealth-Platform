{{ config(materialized='incremental', unique_key=['location_id','y','m']) }}
select
  location_id,
  extract(year from d) as y,
  extract(month from d) as m,
  avg(tavg_c) as tavg_c,
  sum(prcp_mm) as prcp_mm,
  avg(hdd18) as hdd18,
  avg(cdd18) as cdd18,
  avg(gdd10_30) as gdd10_30
from {{ source('silver','climate_silver_daily_features') }}
group by 1,2,3
