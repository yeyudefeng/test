insert overwrite table db_ads_nstc.clife_nstc_ads_order_by_shop_y partition(part_date)
select
     shop_id                    --店铺id
    ,year_date                 --业务日期
    ,shop_name                  --店铺名称
    ,shop_main                  --店铺主营品种
    ,shop_address               --店铺地址
    ,coalesce(bug_product_num,0)           as bug_product_num           --产品购买量
    ,coalesce(bug_goods_num,0)             as bug_goods_num             --商品够买量
    ,coalesce(order_num,0)                 as order_num                 --下单总数
    ,coalesce(discuss_order_num,0)         as discuss_order_num         --商议订单数
    ,coalesce(wait_pay_order_num,0)        as wait_pay_order_num        --待支付订单数
    ,coalesce(wait_delivery_order_num,0)   as wait_delivery_order_num   --待发货订单数
    ,coalesce(wait_receiv_order_num,0)     as wait_receiv_order_num     --待收货订单数
    ,coalesce(wait_evaluation_order_num,0) as wait_evaluation_order_num --待评价订单数
    ,coalesce(return_order_o_num,0)        as return_order_o_num        --退货中订单数
    ,coalesce(return_order_s_num,0)        as return_order_s_num        --退货成功订单数
    ,coalesce(complete_order_num,0)        as complete_order_num        --订单完成订单数
    ,coalesce(complete_cancel_num,0)       as complete_cancel_num       --订单取消订单数
    ,coalesce(order_price_all,0)           as order_price_all           --订单总额
    ,coalesce(sub_money_all,0)             as sub_money_all             --优惠金额
    ,coalesce(payment,0)                   as payment                   --实付金额
    ,coalesce(pay_order_num,0)             as pay_order_num             --支付订单总数
    ,coalesce(no_pay_order_num,0)          as no_pay_order_num          --未支付订单数
    ,coalesce(wechar_pay_order_num,0)      as wechar_pay_order_num      --微信支付订单数
    ,coalesce(alipay_pay_order_num,0)      as alipay_pay_order_num      --支付宝支付订单数
    ,coalesce(app_pay_order_num,0)         as app_pay_order_num         --小程序支付订单数
    ,coalesce(icbc_app_pay_num,0)          as icbc_app_pay_num          --工行app支付订单数
    ,coalesce(icbc_pc_pay_num,0)           as icbc_pc_pay_num           --工行pc支付订单数
    ,coalesce(order_post,0)                as order_post                --邮费总额
    ,coalesce(shipping_order_num,0)        as shipping_order_num        --发货订单数
    ,coalesce(company_shipping_num,0)      as company_shipping_num      --发公司物流类型订单数
    ,coalesce(personal_shipping_num,0)     as personal_shipping_num     --发个人物流类型订单数
    ,coalesce(no_remind_shipping_num,0)    as no_remind_shipping_num    --发货未提醒数
    ,coalesce(remind_shipping_num,0)       as remind_shipping_num       --发货提醒数
    ,coalesce(receiving_order_num,0)       as receiving_order_num       --收货订单数
    ,coalesce(appeal_order_num,0)          as appeal_order_num          --申诉订单数
    ,coalesce(no_deal_appeal_num,0)        as no_deal_appeal_num        --申诉未处理订单数
    ,coalesce(deal_appeal_order_num,0)     as deal_appeal_order_num     --申诉已处理订单数
    ,coalesce(return_order_num,0)          as return_order_num          --退货订单总数
    ,coalesce(back_price_all,0)            as back_price_all            --退款总金额
    ,coalesce(unshipped_refunded_num,0)    as unshipped_refunded_num    --未发货退款订单数
    ,coalesce(shipped_no_received_num,0)   as shipped_no_received_num   --已发货未收到货退款订单数
    ,coalesce(received_back_num,0)         as received_back_num         --收到货仅退款订单数
    ,coalesce(received_pay_back_num,0)     as received_pay_back_num    --收到货退货退款订单数
    ,coalesce(company_back_num,0)          as company_back_num          --退货公司物流订单数
    ,coalesce(personal_back_num,0)         as personal_back_num         --退货个人找车订单数
    ,coalesce(inreview_back_num,0)         as inreview_back_num         --退货审核中订单数
    ,coalesce(agree_back_num,0)            as agree_back_num            --退货同意订单数
    ,coalesce(refuse_back_num,0)           as refuse_back_num           --退货拒绝订单数
    ,coalesce(evaluation_order_num,0)      as evaluation_order_num      --完成评价数
    ,coalesce(no_evaluation_num,0)         as no_evaluation_num         --未完成评价数
    ,coalesce(deal_complete_back_num,0)    as deal_complete_back_num    --退货订单交易完成数
    ,coalesce(deal_complete_no_back_num,0) as deal_complete_no_back_num --没有退货订单交易完成数
    ,coalesce(deal_complete_num,0)         as deal_complete_num         --交易完成订单数
    ,coalesce(evaluation_order_num / order_num,0) as evaluation_rate --订单完成评价类型数量占比
    ,coalesce(no_evaluation_num / order_num,0) as  no_evaluation_rate   --订单未评价类型数量占比
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select 
       shop_id                    --店铺id
      ,substr(date_time,1,4) as year_date         --业务日期
      ,max(shop_name)  as shop_name                --店铺名称
      ,max(shop_main)  as shop_main                --店铺主营品种
      ,max(shop_address) as  shop_address          --店铺地址
      ,sum(bug_product_num)            as bug_product_num          --产品购买量
      ,sum(bug_goods_num)              as bug_goods_num            --商品够买量
      ,sum(order_num)                  as order_num                --下单总数
      ,sum(discuss_order_num)          as discuss_order_num        --商议订单数
      ,sum(wait_pay_order_num)         as wait_pay_order_num       --待支付订单数
      ,sum(wait_delivery_order_num)    as wait_delivery_order_num  --待发货订单数
      ,sum(wait_receiv_order_num)      as wait_receiv_order_num    --待收货订单数
      ,sum(wait_evaluation_order_num)  as wait_evaluation_order_num--待评价订单数
      ,sum(return_order_o_num)         as return_order_o_num       --退货中订单数
      ,sum(return_order_s_num)         as return_order_s_num       --退货成功订单数
      ,sum(complete_order_num)         as complete_order_num       --订单完成订单数
      ,sum(complete_cancel_num)        as complete_cancel_num      --订单取消订单数
      ,sum(order_price_all)            as order_price_all          --订单总额
      ,sum(sub_money_all)              as sub_money_all            --优惠金额
      ,sum(payment)                    as payment                  --实付金额
      ,sum(pay_order_num)              as pay_order_num            --支付订单总数
      ,sum(no_pay_order_num)           as no_pay_order_num         --未支付订单数
      ,sum(wechar_pay_order_num)       as wechar_pay_order_num     --微信支付订单数
      ,sum(alipay_pay_order_num)       as alipay_pay_order_num     --支付宝支付订单数
      ,sum(app_pay_order_num)          as app_pay_order_num        --小程序支付订单数
      ,sum(icbc_app_pay_num)           as icbc_app_pay_num         --工行app支付订单数
      ,sum(icbc_pc_pay_num)            as icbc_pc_pay_num          --工行pc支付订单数
      ,sum(order_post)                 as order_post               --邮费总额
      ,sum(shipping_order_num)         as shipping_order_num       --发货订单数
      ,sum(company_shipping_num)       as company_shipping_num     --发公司物流类型订单数
      ,sum(personal_shipping_num)      as personal_shipping_num    --发个人物流类型订单数
      ,sum(no_remind_shipping_num)     as no_remind_shipping_num   --发货未提醒数
      ,sum(remind_shipping_num)        as remind_shipping_num      --发货提醒数
      ,sum(receiving_order_num)        as receiving_order_num      --收货订单数
      ,sum(appeal_order_num)           as appeal_order_num         --申诉订单数
      ,sum(no_deal_appeal_num)         as no_deal_appeal_num       --申诉未处理订单数
      ,sum(deal_appeal_order_num)      as deal_appeal_order_num    --申诉已处理订单数
      ,sum(return_order_num)           as return_order_num         --退货订单总数
      ,sum(back_price_all)             as back_price_all           --退款总金额
      ,sum(unshipped_refunded_num)     as unshipped_refunded_num   --未发货退款订单数
      ,sum(shipped_no_received_num)    as shipped_no_received_num  --已发货未收到货退款订单数
      ,sum(received_back_num)          as received_back_num        --收到货仅退款订单数
      ,sum(received_pay_back_num)      as received_pay_back_num    --收到货退货退款订单数
      ,sum(company_back_num)           as company_back_num         --退货公司物流订单数
      ,sum(personal_back_num)          as personal_back_num        --退货个人找车订单数
      ,sum(inreview_back_num)          as inreview_back_num        --退货审核中订单数
      ,sum(agree_back_num)             as agree_back_num           --退货同意订单数
      ,sum(refuse_back_num)            as refuse_back_num          --退货拒绝订单数
      ,sum(evaluation_order_num)       as evaluation_order_num     --完成评价数
      ,sum(no_evaluation_num)          as no_evaluation_num        --未完成评价数
      ,sum(deal_complete_back_num )    as deal_complete_back_num   --退货订单交易完成数
      ,sum(deal_complete_no_back_num)  as deal_complete_no_back_num--没有退货订单交易完成数
      ,sum(deal_complete_num)          as deal_complete_num        --交易完成订单数
  from db_dw_nstc.clife_nstc_dws_order_converge
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by shop_id
          ,substr(date_time,1,4)
)t