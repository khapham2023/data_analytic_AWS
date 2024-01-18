{{ config(
    materialized = 'incremental',
    unique_key = 'customer_sk'
)}}

select 
    customer_snapshot.SALUTATION, 
    customer_snapshot.PREFERRED_CUST_FLAG, 
    customer_snapshot.FIRST_SALES_DATE_SK, 
    customer_snapshot.CUSTOMER_SK, 
    customer_snapshot.FIRST_NAME, 
    customer_snapshot.LAST_NAME, 
    customer_snapshot.CUSTOMER_ID, 
    customer_snapshot.BIRTH_MONTH, 
    customer_snapshot.BIRTH_COUNTRY, 
    customer_snapshot.BIRTH_YEAR, 
    customer_snapshot.BIRTH_DAY, 
    customer_snapshot.ADDRESS_SK, 
    customer_snapshot.EMAIL_ADDRESS, 
    customer_snapshot.LOADED_AT, 
    customer_snapshot.DBT_SCD_ID, 
    customer_snapshot.DBT_UPDATED_AT, 
    customer_snapshot.DBT_VALID_FROM, 
    customer_snapshot.DBT_VALID_TO,
    customer_snapshot.DEMO_SK,
    customer_address.STREET_NAME, 
    customer_address.SUITE_NUMBER, 
    customer_address.STATE, 
    customer_address.LOCATION_TYPE, 
    customer_address.COUNTRY, 
    customer_address.COUNTY, 
    customer_address.STREET_NUMBER, 
    customer_address.ZIP, 
    customer_address.CITY, 
    customer_address.STREET_TYPE,
    customer_demographics.DEP_EMPLOYED_COUNT, 
    customer_demographics.DEP_COUNT, 
    customer_demographics.CREDIT_RATING, 
    customer_demographics.EDUCATION_STATUS, 
    customer_demographics.PURCHASE_ESTIMATE, 
    customer_demographics.ARITAL_STATUS, 
    customer_demographics.DEP_COLLEGE_COUNT, 
    customer_demographics.GENDER,
    household_demographics.BUY_POTENTIAL, 
    household_demographics.VEHICLE_COUNT,
    income_band.LOWER_BOUND, 
    income_band.INCOME_BAND_SK, 
    income_band.UPPER_BOUND

from {{ ref('customer_snapshot')}} as customer_snapshot
left join {{ref('stg_customer_address')}} as customer_address using (ADDRESS_SK)
left join {{ ref('stg_customer_demographics')}} as customer_demographics using (DEMO_SK)
left join {{ ref('stg_household_demographics')}} as household_demographics using(DEMO_SK)
left join {{ ref('stg_income_band') }} as income_band using(INCOME_BAND_SK)

{% if is_incremental() %}
where customer_snapshot.DBT_VALID_TO is null
{% endif %}
