insert overwrite table db_dw_nstc.clife_nstc_dwd_order_detail_info partition(part_date) 
select 
     a.detail_id                      -- '订单详情id' 
    ,a.order_id                       -- '订单ID' 
    ,a.product_id                     -- '产品规格ID' 
    ,b.member_id                      -- '买家ID' 
    ,a.goods_id                       -- '商品ID' 
    ,c.goods_name                     -- '商品名称' 
    ,c.goods_title                    -- '商品标题' 
    ,c.class_id                       -- '商品分类' 
    ,c.class_name                     -- '商品分类名称' 
    ,c.type_id                        -- '商品类型' 
    ,c.type_name                      -- '商品类型名称' 
    ,c.brand_id                       -- '商品品牌' 
    ,c.brand_name                     -- '品牌名称' 
    ,c.brand_url                      -- '品牌网址' 
    ,c.shop_id                        -- '店铺id' 
    ,c.shop_name                      -- '店铺名称' 
    ,c.recom_xq                       -- '邮费,0-包邮,1-不包邮' 
    ,d.sku                            -- 'SKU' 
    ,d.product_name                   -- '货品名' 
    ,d.product_img                    -- '产品图片' 
    ,d.product_spec                   -- '货品规格' 
    ,d.product_price                  -- '价格' 
    ,d.product_post                   -- '期货购买价' 
    ,a.total                          -- '购买数量' 
    ,if(e.order_id is not null and e.status = 1, 1, 0) as is_back    -- '是否退货：1 退货，0 未退货'
    ,f.shipping_id                    -- '订单收货信息id' 
    ,f.receiver_province              -- '省份' 
    ,f.receiver_city                  -- '城市' 
    ,f.receiver_area                  -- '区县' 
    ,f.receiver_town                  -- '街道' 
    ,a.create_at                      -- '创建时间' 
    ,a.part_date                      -- '分区日期'
from (
    select 
         detail_id                    -- '订单详情id' 
        ,order_id                     -- '订单ID' 
        ,product_id                   -- '产品规格ID' 
        ,goods_id                     -- '商品ID' 
        ,total                        -- '购买数量' 
        ,create_at                    -- '创建时间' 
        ,part_date                    -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_order_detail 
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
) a 
left join db_ods_nstc.clife_nstc_ods_order_order b 
on a.part_date = b.part_date and a.order_id = b.order_id
left join db_dw_nstc.clife_nstc_dim_goods_goods c 
on a.part_date = c.part_date and a.goods_id = c.goods_id
left join db_dw_nstc.clife_nstc_dim_goods_product d
on a.part_date = d.part_date and a.product_id = d.product_id
left join (
    select 
         order_id                                -- '订单ID'
        ,status                                  -- '退货状态0-审核中,1-同意,2-拒绝'
        ,part_date                               -- '分区日期'
    from
    (
        select 
             order_id                            -- '订单ID'
            ,row_number() over(partition by order_id order by create_at desc) as rank_num    
            ,status                              -- '退货状态0-审核中,1-同意,2-拒绝'
            ,part_date                           -- '分区日期'
        from db_ods_nstc.clife_nstc_ods_order_back
        where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    ) tmp where rank_num = 1
) e
on a.part_date = e.part_date and a.order_id = e.order_id
left join db_ods_nstc.clife_nstc_ods_order_shipping f 
on a.part_date = f.part_date and a.order_id = f.order_id and f.del_flag = 0