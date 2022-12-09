with total_instances as 
(select
    partn,
    count(consultant_id)::real no_of_leads
from {{ref('stg_referral_partners')}}
group by partner_type
order by partner_type),





{# conversions as 
(select
    partner_type,
    count(consultant_id)::real no_of_conversions
from {{ref('stg_referral_partners')}} 
where status = 'successful' 
group by partner_type
order by partner_type)

select
    t.partner_type,
    t.no_of_leads no_of_leads,
    coalesce(c.no_of_conversions, 0) no_of_conversions,
    round(coalesce((c.no_of_conversions*100/ t.no_of_leads :: float), 0)) as conversion_rate
from total_instances t
left join conversions c
on t.partner_type = c.partner_type #}