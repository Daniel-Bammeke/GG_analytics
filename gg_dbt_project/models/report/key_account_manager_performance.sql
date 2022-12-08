select
    lead_sales_contact,
    sum(conversions_per_month) average_monthly_conversions
from {{ref('conversion_per_partner')}}
group by lead_sales_contact
order by 2 desc



