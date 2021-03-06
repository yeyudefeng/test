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