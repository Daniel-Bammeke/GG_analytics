{{ config(
    tags=["staging"]
) }}
with sales_people as
(
    select *
    from {{ source('raw_data', 'sales_people') }}
)

select * from sales_people