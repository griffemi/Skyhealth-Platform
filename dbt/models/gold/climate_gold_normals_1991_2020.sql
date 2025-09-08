{{ config(materialized='table') }}
select
  location_id,
  m,
  avg(tavg_c) as tavg_c,
  avg(prcp_mm) as prcp_mm
from {{ ref('climate_gold_monthly') }}
where y between 1991 and 2020
group by 1,2
