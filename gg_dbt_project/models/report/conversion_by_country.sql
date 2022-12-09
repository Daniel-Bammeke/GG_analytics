select
    sum(partners.leads_per_month) leads_per_month,
    sum(partners.conversions_per_month) conversions_per_month,
    coalesce(sales.country, 'Unknown') country
from {{ref('conversion_per_partner')}} partners
left join {{ref('stg_sales_people')}} sales
on partners.lead_sales_contact = sales.lead_sales_contact
group by 3