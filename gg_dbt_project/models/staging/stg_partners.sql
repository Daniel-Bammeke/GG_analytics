{{ config(
    tags=["staging"]
) }}

with partners as
(
    select *
    from {{ source('raw_data', 'partners') }}
)

select 
    id, 
	to_timestamp((created_at :: numeric)/1E+9) created_at,
	to_timestamp((updated_at :: numeric)/1E+9) updated_at,
	partner_type,
    case
       when lead_sales_contact = '0' then 'Not Assigned'
       else lead_sales_contact
    end lead_sales_contact
from partners