{{ config(materialized='incremental', unique_key=['location_id','y','m']) }}
select m.*, (m.tavg_c - n.tavg_c) as tavg_c_anom
from {{ ref('climate_gold_monthly') }} m
left join {{ ref('climate_gold_normals_1991_2020') }} n
  using (location_id, m)
