{{ config(
    tags=["referral", "kpi"]
) }}

select
    consultant_id,
    count(distinct(company_id)) no_of_referrals
from {{ref('stg_referral_partners')}}
group by consultant_id
order by 2 desc

