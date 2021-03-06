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