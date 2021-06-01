-- an amazing article: https://discourse.getdbt.com/t/generating-an-auto-incrementing-id-in-dbt/579/2

with deal_with_incrementing_pkey as (
  select * from {{ ref('deal_with_incrementing_pkey')}}
)


select {{ dbt_utils.surrogate_key(['emailid', 'postcode']) }} as surrogate_key,
emailid,
postcode
from deal_with_incrementing_pkey

