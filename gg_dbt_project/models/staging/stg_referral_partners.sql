{{ config(
    tags=["staging","referral"]
) }}

select 
    referrals.*,
    coalesce(partners.partner_type,'Not Available') partner_type,
    coalesce(partners.lead_sales_contact,'Not Available') lead_sales_contact
from {{ref('stg_referrals')}} referrals
left join {{ref('stg_partners')}} partners
on referrals.partner_id = partners.id