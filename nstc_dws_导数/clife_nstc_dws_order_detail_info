insert overwrite table db_dw_nstc.clife_nstc_dws_order_detail_info partition(part_date)
select                                                  
     member_id                                                               -- '买家id'
    ,product_id                                                              -- '产品id'
    ,goods_id                                                                -- '商品id'
    ,class_id                                                                -- '商品分类'
    ,type_id                                                                 -- '商品类型'
    ,receiver_province                                                       -- '省份'
    ,receiver_city                                                           -- '城市'
    ,receiver_area                                                           -- '区县'
    ,receiver_town                                                           -- '街道'
    ,substr(create_at,0,10)                           as order_detail_day    -- '订单创建日期'
    ,max(class_name)                                  as class_name          -- '商品分类名称' 
    ,max(type_name)                                   as type_name           -- '商品类型名称' 
    ,max(shop_id)                                     as shop_id             -- '店铺id' 
    ,max(product_name)                                as product_name        -- '货品名' 
    ,count(1)                                         as sale_cnt            -- '购买次数'
    ,sum(total)                                       as total_cnt           -- '购买总量'
    ,sum(recom_xq)                                    as total_recom_xq      -- '邮费总额'
    ,count(distinct sku)                              as sku_cnt             -- 'SKU数'
    ,sum(total * product_price)                       as total_price         -- '消费总额'
    ,sum(case when is_back = 1 then total else 0 end) as total_back_cnt      -- '退货数量'
    ,sum(case when is_back = 1 then 1 else 0 end)     as back_cnt            -- '退货次数'
    ,max(part_date)                                   as part_date           -- '分区日期'
from db_dw_nstc.clife_nstc_dwd_order_detail_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by  member_id, product_id ,goods_id, class_id, type_id, receiver_province, receiver_city, receiver_area, receiver_town, substr(create_at,0,10)