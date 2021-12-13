===========================
    clife_nstc_dwd_deal_message_info
===========================
insert overwrite table db_dw_nstc.clife_nstc_dwd_deal_message_info partition(part_date) 
select 
 a.id                                             -- '消息发布id'       
,a.title                                          -- '标题'
,a.member_id                                      -- '会员id'
,b.member_name                                    -- '昵称' 
,a.goods_class                                    -- '种类'
,a.goods_num                                      -- '买卖数量'
,a.price                                          -- '交易价格'
,a.start_place                                    -- '开始地'
,a.end_place                                      -- '目的地'
,a.image                                          -- '图片'
,a.comment                                        -- '备注内容'
,a.status                                         -- '发布状态' 
,a.scannum                                        -- '浏览量'
,a.action_type                                    -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
,a.create_at                                      -- '创建时间'
,a.update_at                                      -- '更新时间'
,a.part_date                                      -- '分区日期'
from (
    select 
         purchase_id                as id               -- '消息发布id'       
        ,null                       as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,purchase_class             as goods_class      -- '种类'
        ,purchase_num               as goods_num        -- '买卖数量'
        ,purchase_price             as price            -- '交易价格'
        ,fromword                   as start_place      -- '开始地'
        ,go                         as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,conent                     as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,0                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_apply_purchase
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         suppery_Id                 as id               -- '消息发布id'       
        ,null                       as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,goodclass                  as goods_class      -- '种类'
        ,supperNum                  as goods_num        -- '买卖数量'
        ,cast(supper_price as double) as price            -- '交易价格'
        ,fromword                   as start_place      -- '开始地'
        ,go                         as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,conment                    as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,1                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_apply_supply
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         rent_id                    as id               -- '消息发布id'       
        ,title                      as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,null                       as goods_class      -- '种类'
        ,null                       as goods_num        -- '买卖数量'
        ,price                      as price            -- '交易价格'
        ,stcity                     as start_place      -- '开始地'
        ,encity                     as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,null                       as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,2                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
        from db_ods_nstc.clife_nstc_ods_storage_rent
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         lease_id                   as id               -- '消息发布id'       
        ,title                      as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,null                       as goods_class      -- '种类'
        ,null                       as goods_num        -- '买卖数量'
        ,price                      as price            -- '交易价格'
        ,stcity                     as start_place      -- '开始地'
        ,encity                     as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,null                       as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,3                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_storage_lease
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
) a 
left join db_dw_nstc.clife_nstc_dim_member_user b
on a.part_date = b.part_date and a.member_id = b.member_id






===========================
    clife_nstc_dwd_order_info
===========================
insert overwrite table db_dw_nstc.clife_nstc_dwd_order_info partition(part_date)
select
     a.order_id                       --订单id
    ,a.shop_id                        --店铺id
    ,b.shop_name                      --店铺名称
    ,a.member_id                      --会员ID
    ,c.member_name                    --会员名称
    ,k.good_ids                         --商品ids
    ,k.product_ids                      --产品ids
    ,a.order_sn                       --订单号
    ,a.merge_sn                       --合单号
    ,a.status      as order_status    --订单状态
    ,a.order_price                    --订单总额
    ,a.create_at   as order_create_time --下单时间
    ,a.update_at   as order_update_time --订单更新时间
    ,a.coupon_id          --优惠券id
    ,d.coupon_name        --优惠券名称
    ,d.sub_money          --优惠金额
    ,a.payment            --实付金额
    ,case when a.pay_type is null then 0 else  a.pay_type end as pay_type --支付类型
    ,a.pay_at             --支付时间
    ,a.order_post         --发货邮费
    ,a.shipping_type      --发货物流类型
    ,a.shipping_name      --发货物流名称
    ,a.shipping_reminder  --发货提醒
    ,a.consign_at         --发货时间
    ,e.shipping_id        --订单收货信息id
    ,e.receiver_name      --收件人
    ,e.receiver_phone     --电话
    ,e.receiver_province  --省份
    ,e.receiver_city      --城市
    ,e.receiver_area      --区县
    ,e.receiver_town      --街道
    ,e.receiver_address   --地址
    ,a.receiving_at       --收货时间
    ,a.coment             --是否评价
    ,f.id             as appeal_id        --申诉id
    ,f.reason_content as appeal_reason    --申诉原因
    ,f.status         as appeal_status    --申诉状态
    ,f.create_at      as appeal_create_at --申诉时间
    ,g.back_id                            --退换货id
    ,g.status         as back_status      --退货审核状态
    ,g.price          as back_price       --退款金额
    ,g.back_type                          --退货类型
    ,g.reason        as back_reason       --退货原因
    ,g.create_at     as return_at         --退货时间(跟退货表的时间是否相同)
    ,g.type          as back_way          --退货方式
    ,g.express_name  as back_express_name --退货物流名
    ,g.express_code  as back_express_code --退货快物流单号
    ,a.end_at        as order_end_time    --订单交易完成时间
    ,a.close_at      as order_close_time  --订单交易关闭时间
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_nstc.clife_nstc_ods_order_order a
left join db_dw_nstc.clife_nstc_dim_shop_shop b on a.shop_id = b.shop_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_nstc.clife_nstc_dim_member_user c on a.member_id = c.member_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_nstc.clife_nstc_dim_shop_coupon d on a.coupon_id = d.id and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_nstc.clife_nstc_ods_order_shipping e on a.order_id = e.order_id and e.del_flag = 0 and e.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
  select *
  from
  (
    select st.*,row_number()over(partition by order_id order by create_at desc) as ranks
    from db_ods_nstc.clife_nstc_ods_order_appeal st
    where del_flag = 0 
      and part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )ls
  where ranks = 1
)f on a.order_id = f.order_id
left join 
(
  select *
  from
  (
    select sb.*,row_number()over(partition by order_id order by update_at desc) as ranks
    from db_ods_nstc.clife_nstc_ods_order_back sb
    where del_flag = 0 
      and part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )ls
  where ranks = 1
)g on a.order_id = g.order_id
left join 
(
  select
       order_id
      ,concat_ws(',',collect_list(cast(product_id as string))) as product_ids
      ,concat_ws(',',collect_list(cast(goods_id as string))) as good_ids
  from db_ods_nstc.clife_nstc_ods_order_detail
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and del_flag=0
  group by order_id
)k on a.order_id = k.order_id
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and a.del_flag=0


===========================
    clife_nstc_dwd_event_info
===========================
insert overwrite table db_dw_nstc.clife_nstc_dwd_event_info partition(part_date)
select 
     t.id             --记录id
    ,t.member_id      --会员id
    ,c.member_name    --会员名称
    ,t.`object_id`    --被操作对象id （商品 店铺）
    ,t.`object_name`  --被操作对象名称
    ,t.object_type    --对象类型（0 商品 1 店铺）
    ,t.behavior_type  --行为动作类型
    ,t.`type`         --是否是新客户
    ,t.create_at      --行为时间
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select
       a.id                             -- 记录id
      ,a.member_id                      -- 会员id
      ,a.goods_id   as `object_id`      -- 被操作对象id
      ,b.goods_name as `object_name`    -- 被操作对象名称
      ,'商品'  as object_type               -- 对象类型（0 商品 1 店铺）
      ,0   as behavior_type
      ,a.`type`                         -- 新老访客数0-新访客,1-老访客
      ,a.create_at                      -- 访问日期
  from db_ods_nstc.clife_nstc_ods_record_goods_member a 
  left join db_dw_nstc.clife_nstc_dim_goods_goods b on a.goods_id = b.goods_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  union all
  select
       a.id                             -- 记录id
      ,a.member_id                      -- 会员id
      ,a.shop_id    as `object_id`      -- 被操作对象id
      ,b.shop_name  as `object_name`    -- 被操作对象名称
      ,'店铺'    as object_type               -- 对象类型（0 商品 1 店铺）
      ,0   as  behavior_type
      ,a.`type`                         -- 新老访客0-新访客,1-老访客
      ,a.create_at                      -- 访问日期
  from db_ods_nstc.clife_nstc_ods_record_shop_member a 
  left join db_dw_nstc.clife_nstc_dim_shop_shop b on a.shop_id = b.shop_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  union all
  select
       a.id                             -- 记录id
      ,a.member_id                      -- 会员id
      ,a.shop_id    as `object_id`      -- 被操作对象id
      ,b.shop_name  as `object_name`    -- 被操作对象名称
      ,'店铺'   as object_type               -- 对象类型（0 商品 1 店铺）
      ,1   as  behavior_type
      ,null as  `type`                  -- 新老访客0-新访客,1-老访客
      ,a.create_at                      -- 关注日期
  from db_ods_nstc.clife_nstc_ods_member_shop a 
  left join db_dw_nstc.clife_nstc_dim_shop_shop b on a.shop_id = b.shop_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  union all
  select
       a.follow_id as id                -- 记录id
      ,a.member_id                      -- 会员id
      ,a.goods_id   as `object_id`      -- 被操作对象id
      ,b.goods_name as `object_name`    -- 被操作对象名称
      ,'商品'   as object_type               -- 对象类型（0 商品 1 店铺）
      ,1   as behavior_type
      ,null as `type`                   -- 新老访客数0-新访客,1-老访客
      ,a.create_at                      -- 访问日期
  from db_ods_nstc.clife_nstc_ods_member_follow a 
  left join db_dw_nstc.clife_nstc_dim_goods_goods b on a.goods_id = b.goods_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
)t
left join db_dw_nstc.clife_nstc_dim_member_user c on t.member_id = c.member_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_nstc_dwd_order_detail_comment_info
===========================
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


===========================
    clife_nstc_dwd_order_detail_info
===========================
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


