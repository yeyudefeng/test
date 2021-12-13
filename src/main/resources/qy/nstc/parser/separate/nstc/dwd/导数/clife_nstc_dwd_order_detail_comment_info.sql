insert overwrite table db_dw_nstc.clife_nstc_dwd_order_detail_comment_info partition(part_date) 
select 
     a.comment_id                     -- '评论表id' 
    ,a.product_id                     -- '产品id' 
    ,a.goods_id                       -- '商品id' 
    ,a.member_id                      -- '评论人' 
    ,a.star                           -- '评价星级 1-5,默认5星' 
    ,a.content                        -- '评价内容' 
    ,a.comment_img                    -- '评论图片' 
    ,c.receiver_province              -- '省份' 
    ,c.receiver_city                  -- '城市' 
    ,c.receiver_area                  -- '区县' 
    ,c.receiver_town                  -- '街道' 
    ,c.receiver_address               -- '地址'     
    ,a.create_at                      -- '创建时间' 
    ,a.part_date                      -- '分区日期'
from (
    select 
         comment_id                   -- '评论表id'
        ,detail_id                    -- '订单详情表id' 
        ,product_id                   -- '产品id' 
        ,goods_id                     -- '商品id' 
        ,member_id                    -- '评论人' 
        ,star                         -- '评价星级 1-5,默认5星' 
        ,content                      -- '评价内容' 
        ,comment_img                  -- '评论图片'     
        ,create_at                    -- '创建时间' 
        ,part_date                    -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_order_coment  
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 and disply = 0
) a 
left join db_ods_nstc.clife_nstc_ods_order_detail b
on a.part_date = b.part_date and a.detail_id = b.detail_id and b.del_flag = 0
left join db_ods_nstc.clife_nstc_ods_order_shipping c 
on b.part_date = c.part_date and b.order_id = c.order_id and c.del_flag = 0