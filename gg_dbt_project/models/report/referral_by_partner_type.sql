select
    partner_type,
    count(distinct(company_id)) no_of_referrals
from {{ref('stg_referral_partners')}}
group by partner_type