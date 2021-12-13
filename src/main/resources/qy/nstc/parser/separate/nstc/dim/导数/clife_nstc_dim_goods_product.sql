insert overwrite table db_dw_nstc.clife_nstc_dim_goods_product partition(part_date)
select
     product_id                     -- '产品id' 
    ,batch_id                       -- '批次id' 
    ,sku                            -- 'SKU' 
    ,goods_id                       -- '商品ID' 
    ,product_name                   -- '货品名' 
    ,product_img                    -- '产品图片' 
    ,product_spec                   -- '货品规格' 
    ,product_price                  -- '价格' 
    ,product_post                   -- '期货购买价' 
    ,product_weight                 -- '一包的重量' 
    ,product_buy_min                -- '期货卖出一包的重量' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,store_name                     -- '仓库名' 
    ,product_loading
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_product
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
