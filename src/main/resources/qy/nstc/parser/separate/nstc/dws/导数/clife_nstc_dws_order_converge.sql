insert overwrite table db_dw_nstc.clife_nstc_dws_order_converge partition(part_date)
select 
     cm.member_id         --会员ID
    ,cm.shop_id           --店铺id
    ,cm.receiver_province --省份
    ,cm.receiver_city     --城市
    ,cm.receiver_area     --区县
    ,cm.receiver_town     --街道
    ,cm.date_w as  date_time  --业务日期
    ,k.member_name         --会员名称
    ,k.`level` as member_identity --身份类型
    ,p.shop_name                  --店铺名称
    ,p.shop_main                  --店铺主营品种
    ,concat(p.shop_province,p.shop_city,p.shop_area,p.shop_town,p.shop_address) as shop_address --店铺地址
    ,coalesce(a.bug_product_num,0)           as bug_product_num           --产品购买量
    ,coalesce(a.bug_goods_num,0)             as bug_goods_num             --商品够买量
    ,coalesce(a.order_num,0)                 as order_num                 --下单总数
    ,coalesce(a.discuss_order_num,0)         as discuss_order_num         --商议订单数
    ,coalesce(a.wait_pay_order_num,0)        as wait_pay_order_num        --待支付订单数
    ,coalesce(a.wait_delivery_order_num,0)   as wait_delivery_order_num   --待发货订单数
    ,coalesce(a.wait_receiv_order_num,0)     as wait_receiv_order_num     --待收货订单数
    ,coalesce(a.wait_evaluation_order_num,0) as wait_evaluation_order_num --待评价订单数
    ,coalesce(a.return_order_o_num,0)        as return_order_o_num        --退货中订单数
    ,coalesce(a.return_order_s_num,0)        as return_order_s_num        --退货成功订单数
    ,coalesce(a.complete_order_num,0)        as complete_order_num        --订单完成订单数
    ,coalesce(a.complete_cancel_num,0)       as complete_cancel_num       --订单取消订单数
    ,coalesce(a.order_price_all,0)           as order_price_all           --订单总额
    ,coalesce(a.sub_money_all,0)             as sub_money_all             --优惠金额
    ,coalesce(b.payment,0)                   as payment                   --实付金额
    ,coalesce(b.pay_order_num,0)             as pay_order_num             --支付订单总数
    ,coalesce(b.no_pay_order_num,0)          as no_pay_order_num          --未支付订单数
    ,coalesce(b.wechar_pay_order_num,0)      as wechar_pay_order_num      --微信支付订单数
    ,coalesce(b.alipay_pay_order_num,0)      as alipay_pay_order_num      --支付宝支付订单数
    ,coalesce(b.app_pay_order_num,0)         as app_pay_order_num         --小程序支付订单数
    ,coalesce(b.icbc_app_pay_num,0)          as icbc_app_pay_num          --工行app支付订单数
    ,coalesce(b.icbc_pc_pay_num,0)           as icbc_pc_pay_num           --工行pc支付订单数
    ,coalesce(a.order_post,0)                as order_post                --邮费总额
    ,coalesce(c.shipping_order_num,0)        as shipping_order_num        --发货订单数
    ,coalesce(c.company_shipping_num,0)      as company_shipping_num      --发公司物流类型订单数
    ,coalesce(c.personal_shipping_num,0)     as personal_shipping_num     --发个人物流类型订单数
    ,coalesce(c.no_remind_shipping_num,0)    as no_remind_shipping_num    --发货未提醒数
    ,coalesce(c.remind_shipping_num,0)       as remind_shipping_num       --发货提醒数
    ,coalesce(d.receiving_order_num,0)       as receiving_order_num       --收货订单数
    ,coalesce(f.appeal_order_num,0)          as appeal_order_num          --申诉订单数
    ,coalesce(f.no_deal_appeal_num,0)        as no_deal_appeal_num        --申诉未处理订单数
    ,coalesce(f.deal_appeal_order_num,0)     as deal_appeal_order_num     --申诉已处理订单数
    ,coalesce(g.return_order_num,0)          as return_order_num          --退货订单总数
    ,coalesce(g.back_price_all,0)            as back_price_all            --退款总金额
    ,coalesce(g.unshipped_refunded_num,0)    as unshipped_refunded_num    --未发货退款订单数
    ,coalesce(g.shipped_no_received_num,0)   as shipped_no_received_num   --已发货未收到货退款订单数
    ,coalesce(g.received_back_num,0)         as received_back_num         --收到货仅退款订单数
    ,coalesce(g.received_pay_back_num,0)     as received_pay_back_num     --收到货退货退款订单数
    ,coalesce(g.company_back_num,0)          as company_back_num          --退货公司物流订单数
    ,coalesce(g.personal_back_num,0)         as personal_back_num         --退货个人找车订单数
    ,coalesce(g.inreview_back_num,0)         as inreview_back_num         --退货审核中订单数
    ,coalesce(g.agree_back_num,0)            as agree_back_num            --退货同意订单数
    ,coalesce(g.refuse_back_num,0)           as refuse_back_num           --退货拒绝订单数
    ,coalesce(e.evaluation_order_num,0)      as evaluation_order_num      --完成评价数
    ,coalesce(e.no_evaluation_num,0)         as no_evaluation_num         --未完成评价数
    ,coalesce(e.deal_complete_back_num,0)    as deal_complete_back_num    --退货订单交易完成数
    ,coalesce(e.deal_complete_no_back_num,0) as deal_complete_no_back_num --没有退货订单交易完成数
    ,coalesce(e.deal_complete_num,0)         as deal_complete_num         --交易完成订单数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from 
(
  select * 
  from 
  (
    select date_w 
    from db_dw_nstc.clife_nstc_dim_date 
    where dt_code<=regexp_replace(date_sub(current_date(),1),'-','') and dt_code>='20210101'
  ) tdate
  left join 
  ( 
    select
        member_id
       ,shop_id
       ,receiver_province
       ,receiver_city
       ,receiver_area
       ,max(receiver_town) as receiver_town
       ,count(1) as cnt
    from db_dw_nstc.clife_nstc_dwd_order_info
    where part_date = regexp_replace(date_sub(from_unixtime(unix_timestamp() ,'yyyy-MM-dd'),1),'-','') 
    group by member_id
            ,shop_id
            ,receiver_province
            ,receiver_city
            ,receiver_area
   ) om on 1=1
) cm
left join 
(
   select 
        member_id
       ,shop_id
       ,receiver_province
       ,receiver_city
       ,receiver_area
       ,max(receiver_town) as receiver_town
       ,substr(order_create_time,1,10)     as order_create_day --下单时间
       ,sum(size(split(product_ids,',')))       as bug_product_num  --产品购买量
       ,sum(size(split(good_ids,',')))          as bug_goods_num    --商品够买量
       ,count(1)                           as order_num        --下单总数
       ,sum(case when order_status = 0 then 1 else 0 end) as discuss_order_num         --商议订单数
       ,sum(case when order_status = 1 then 1 else 0 end) as wait_pay_order_num        --待支付订单数
       ,sum(case when order_status = 2 then 1 else 0 end) as wait_delivery_order_num   --待发货订单数
       ,sum(case when order_status = 3 then 1 else 0 end) as wait_receiv_order_num     --待收货订单数
       ,sum(case when order_status = 4 then 1 else 0 end) as wait_evaluation_order_num --待评价订单数
       ,sum(case when order_status = 5 then 1 else 0 end) as return_order_o_num        --退货中订单数
       ,sum(case when order_status = 6 then 1 else 0 end) as return_order_s_num        --退货成功订单数
       ,sum(case when order_status = 7 then 1 else 0 end) as complete_order_num        --订单完成订单数
       ,sum(case when order_status = 8 then 1 else 0 end) as complete_cancel_num       --订单取消订单数
       ,sum(order_price) as order_price_all --订单总额
       ,sum(sub_money)   as sub_money_all   --优惠金额
       ,sum(order_post)  as order_post      --邮费总额
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(order_create_time,1,10)
)a on cm.member_id=a.member_id and cm.shop_id=a.shop_id and cm.receiver_province=a.receiver_province and cm.receiver_city=a.receiver_city and cm.receiver_area=a.receiver_area and cm.date_w=a.order_create_day 
left join 
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(pay_at,1,10) as pay_time_day --支付时间
      ,sum(case when pay_at is not null then 1 else 0 end) as pay_order_num --支付订单总数
      ,sum(case when order_status = 2 or order_status = 3 or order_status = 4 or order_status = 5 or order_status = 7 then payment else 0 end)     as payment         --交易金额
      ,sum(case when pay_type = 0 then 1 else 0 end) as no_pay_order_num      --未支付订单数
      ,sum(case when pay_type = 1 then 1 else 0 end) as wechar_pay_order_num  --微信支付订单数
      ,sum(case when pay_type = 2 then 1 else 0 end) as alipay_pay_order_num  --支付宝支付订单数
      ,sum(case when pay_type = 3 then 1 else 0 end) as app_pay_order_num     --小程序支付订单数
      ,sum(case when pay_type = 4 then 1 else 0 end) as icbc_app_pay_num      --工行app支付订单数
      ,sum(case when pay_type = 5 then 1 else 0 end) as icbc_pc_pay_num       --工行pc支付订单数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(pay_at,1,10)
)b on cm.member_id=b.member_id and cm.shop_id=b.shop_id and cm.receiver_province=b.receiver_province and cm.receiver_city=b.receiver_city and cm.receiver_area=b.receiver_area and cm.date_w=b.pay_time_day 
left join
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(consign_at,1,10) as consign_time_day --发货时间
      ,sum(case when consign_at is not null then 1 else 0 end) as shipping_order_num     --发货订单数
      ,sum(case when shipping_type=0 then 1 else 0 end)        as company_shipping_num   --发公司物流类型订单数
      ,sum(case when shipping_type=1 then 1 else 0 end)        as personal_shipping_num  --发个人物流类型订单数
      ,sum(case when shipping_reminder=0 then 1 else 0 end)    as no_remind_shipping_num --发货未提醒数
      ,sum(case when shipping_reminder=1 then 1 else 0 end)    as remind_shipping_num    --发货提醒数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(consign_at,1,10)

)c on cm.member_id=c.member_id and cm.shop_id=c.shop_id and cm.receiver_province=c.receiver_province and cm.receiver_city=c.receiver_city and cm.receiver_area=c.receiver_area and cm.date_w=c.consign_time_day 
left join
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(receiving_at,1,10) as receiving_time_day --收货时间
      ,sum(case when receiving_at is not null then 1 else 0 end) as receiving_order_num     --收货订单数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(receiving_at,1,10)
)d on cm.member_id=d.member_id and cm.shop_id=d.shop_id and cm.receiver_province=d.receiver_province and cm.receiver_city=d.receiver_city and cm.receiver_area=d.receiver_area and cm.date_w=d.receiving_time_day   
left join
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(appeal_create_at,1,10) as appeal_time_day --申诉时间
      ,sum(case when appeal_create_at is not null then 1 else 0 end) as appeal_order_num --申诉订单数
      ,sum(case when appeal_status=0 then 1 else 0 end) as no_deal_appeal_num        --申诉未处理订单数
      ,sum(case when appeal_status=1 then 1 else 0 end) as deal_appeal_order_num     --申诉已处理订单数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(appeal_create_at,1,10)
)f on cm.member_id=f.member_id and cm.shop_id=f.shop_id and cm.receiver_province=f.receiver_province and cm.receiver_city=f.receiver_city and cm.receiver_area=f.receiver_area and cm.date_w=f.appeal_time_day
left join
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(return_at,1,10) as return_time_day --退货时间
      ,sum(case when return_at is not null and back_status<>2 then 1 else 0 end) as return_order_num --退货订单总数
      ,sum(back_price) as back_price_all --退款总金额
      ,sum(case when back_type=0 then 1 else 0 end)   as unshipped_refunded_num  --未发货退款订单数
      ,sum(case when back_type=1 then 1 else 0 end)   as shipped_no_received_num --已发货未收到货退款订单数
      ,sum(case when back_type=2 then 1 else 0 end)   as received_back_num       --收到货仅退款订单数
      ,sum(case when back_type=3 then 1 else 0 end)   as received_pay_back_num   --收到货退货退款订单数
      ,sum(case when back_way=0 then 1 else 0 end)    as company_back_num        --退货公司物流订单数
      ,sum(case when back_way=1 then 1 else 0 end)    as personal_back_num       --退货个人找车订单数
      ,sum(case when back_status=0 then 1 else 0 end) as inreview_back_num       --退货审核中订单数
      ,sum(case when back_status=1 then 1 else 0 end) as agree_back_num          --退货同意订单数
      ,sum(case when back_status=2 then 1 else 0 end) as refuse_back_num         --退货拒绝订单数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(return_at,1,10)
)g on cm.member_id=g.member_id and cm.shop_id=g.shop_id and cm.receiver_province=g.receiver_province and cm.receiver_city=g.receiver_city and cm.receiver_area=g.receiver_area and cm.date_w=g.return_time_day 
left join
(
  select 
       member_id
      ,shop_id
      ,receiver_province
      ,receiver_city
      ,receiver_area
      ,max(receiver_town) as receiver_town
      ,substr(order_end_time,1,10) as end_time_day --交易完成时间
      ,sum(case when order_end_time is not null then 1 else 0 end) as deal_complete_num --交易完成订单数
      ,sum(case when coment=0 then 1 else 0 end) as no_evaluation_num          --未完成评价数
      ,sum(case when coment=1 then 1 else 0 end) as evaluation_order_num       --完成评价数
      ,sum(case when return_at is not null then 1 else 0 end) as  deal_complete_back_num --退货订单交易完成数
      ,sum(case when return_at is null then 1 else 0 end) as  deal_complete_no_back_num  --没有退货订单交易完成数
  from db_dw_nstc.clife_nstc_dwd_order_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by member_id
          ,shop_id
          ,receiver_province
          ,receiver_city
          ,receiver_area
          ,substr(order_end_time,1,10)
)e on cm.member_id=e.member_id and cm.shop_id=e.shop_id and cm.receiver_province=e.receiver_province and cm.receiver_city=e.receiver_city and cm.receiver_area=e.receiver_area and cm.date_w=e.end_time_day
left join db_dw_nstc.clife_nstc_dim_member_user k on cm.member_id = k.member_id and k.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_nstc.clife_nstc_dim_shop_shop p on cm.shop_id = p.shop_id and p.part_date=regexp_replace(date_sub(current_date(),1),'-','')