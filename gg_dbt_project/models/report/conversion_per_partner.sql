{{ config(
    tags=["conversion", "partner", "month", "kpi"]
) }}

with total_instances as 
(select
    consultant_id,
    count(consultant_id)::real no_of_leads
from {{ref('stg_referral_partners')}}
group by consultant_id
order by consultant_id),

conversions as 
(select
    consultant_id,
    count(consultant_id)::real no_of_conversions
from {{ref('stg_referral_partners')}} 
where status = 'successful' 
group by consultant_id
order by consultant_id),

conversion_rate as
(select
    t.consultant_id,
    t.no_of_leads no_of_leads,
    coalesce(c.no_of_conversions, 0) no_of_conversions,
    round(coalesce((c.no_of_conversions*100/ t.no_of_leads :: float), 0)) as conversion_rate
from total_instances t
left join conversions c
on t.consultant_id = c.consultant_id
order by consultant_id),

months_of_active_operation as
(
select consultant_id,
    extract(year from age(max(updated_at), min(created_at))) * 12 +
extract(month from age(max(updated_at), min(created_at))) actual_month_since_first_lead
from {{ref('stg_referrals')}}
group by consultant_id
order by consultant_id),

non_zero_month as(
select
    conversion_rate.*,
    case
        when months_of_active_operation.actual_month_since_first_lead = 0 then 1
        else months_of_active_operation.actual_month_since_first_lead
    end month_since_first_lead
from conversion_rate
left join months_of_active_operation 
on conversion_rate.consultant_id = months_of_active_operation.consultant_id
order by consultant_id),


sales_sort as
(
    select 
        consultant_id,
        lead_sales_contact
    from {{ref('stg_referral_partners')}}
    group by 1,2
)

select
    conversion_rate.*,
    non_zero_month.month_since_first_lead,
    conversion_rate.no_of_leads/ non_zero_month.month_since_first_lead leads_per_month,
    conversion_rate.no_of_conversions/ non_zero_month.month_since_first_lead conversions_per_month,
    sales_sort.lead_sales_contact
from conversion_rate
left join non_zero_month 
on conversion_rate.consultant_id = non_zero_month.consultant_id
left join sales_sort
on non_zero_month.consultant_id = sales_sort.consultant_id
order by conversions_per_month desc
