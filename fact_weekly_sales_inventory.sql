{{ config(
    materialized = 'incremental',
    unique_key = ['WAREHOUSE_SK', 'ITEM_SK','SOLD_WK_SK']
)}}

with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_PROFIT) AS SUM_PROFIT_WK
FROM {{ref('int_sales_daily')}}
GROUP BY
    1,2,3,4
),

finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week as daily_sales 
left JOIN {{ ref('stg_date_dim')}} as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

populating_friday_date_for_the_week as (
SELECT 
    *,
    date.date_sk as friday_sk
FROM
    finding_first_date_of_the_week as daily_sales
left JOIN {{ ref('stg_date_dim')}} as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=5
)

select 
       t1.warehouse_sk, 
       t1.item_sk, 
       max(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num ,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from populating_friday_date_for_the_week t1
left join {{ref('stg_inventory')}} as inv
    on inv.date_sk = t1.friday_sk and inv.item_sk = t1.item_sk and inv.warehouse_sk = t1.warehouse_sk

{% if is_incremental() %}
    where sold_wk_sk >= (select max(coalesce(sold_wk_sk,0)) from {{this}}) 
{% endif %}

group by 1, 2, 4, 5
having sum(sum_qty_wk) > 0
