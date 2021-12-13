
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_info_m partition(part_date)
select
     product_id                                               -- '产品id'
    ,substr(order_detail_day,0,7)   as order_detail_month     -- '订单创建日期（月）'
    ,max(product_name)              as product_name           -- '货品名' 
    ,sum(sale_cnt      )            as sale_cnt               -- '购买次数'
    ,sum(total_cnt     )            as total_cnt              -- '购买总量'
    ,sum(if(total_recom_xq is null, 0, total_recom_xq))  as total_recom_xq        -- '邮费总额'
    ,sum(sku_cnt       )            as sku_cnt               -- 'SKU数'
    ,sum(if(total_price is null, 0, total_price)   )     as total_price           -- '消费总额'
    ,sum(total_back_cnt)            as total_back_cnt        -- '退货数量'
    ,sum(back_cnt      )            as back_cnt              -- '退货次数'
    ,max(part_date)                 as part_date             -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, substr(order_detail_day,0,7)