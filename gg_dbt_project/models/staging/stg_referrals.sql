{{ config(
    tags=["staging","referral"]
) }}

with referrals as
(
    select *
    from {{ source('raw_data', 'referrals') }}
)

select
    id,
    to_timestamp((created_at :: numeric)/1E+9) created_at,
	to_timestamp((updated_at :: numeric)/1E+9) updated_at,
    company_id,
	partner_id,
	consultant_id,
	status,
	is_outbound
from referrals