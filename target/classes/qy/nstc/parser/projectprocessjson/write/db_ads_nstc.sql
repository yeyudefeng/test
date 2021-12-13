====================================================================================================
==========          clife_nstc_ads_deal_message_info_d                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_deal_message_info_d partition(part_date)
select 
     member_id                                                    -- '会员id'  
    ,release_day                                                  -- '发布时间（日）'
    ,max(member_name)          as member_name                     -- '昵称'
    ,sum(purchase_cnt        ) as purchase_cnt                    -- '采购信息数量'
    ,sum(suppery_cnt         ) as suppery_cnt                     -- '供应信息数量'
    ,sum(rent_cnt            ) as rent_cnt                        -- '车找苗信息数量'
    ,sum(lease_cnt           ) as lease_cnt                       -- '苗找车信息数量'
    ,sum(purchase_release_cnt) as purchase_release_cnt            -- '采购信息发布中数量'
    ,sum(suppery_release_cnt ) as suppery_release_cnt             -- '供应信息发布中数量'
    ,sum(rent_release_cnt    ) as rent_release_cnt                -- '车找苗信息发布中数量'
    ,sum(lease_release_cnt   ) as lease_release_cnt               -- '苗找车信息发布中数量'
    ,sum(purchase_finish_cnt ) as purchase_finish_cnt             -- '采购信息已结束数量'
    ,sum(suppery_finish_cnt  ) as suppery_finish_cnt              -- '供应信息已结束数量'
    ,sum(rent_finish_cnt     ) as rent_finish_cnt                 -- '车找苗信息已结束数量'
    ,sum(lease_finish_cnt    ) as lease_finish_cnt                -- '苗找车信息已结束数量'
    ,sum(purchase_return_cnt ) as purchase_return_cnt             -- '采购信息被驳回数量'
    ,sum(suppery_return_cnt  ) as suppery_return_cnt              -- '供应信息被驳回数量'
    ,sum(rent_return_cnt     ) as rent_return_cnt                 -- '车找苗信息被驳回数量'
    ,sum(lease_return_cnt    ) as lease_return_cnt                -- '苗找车信息被驳回数量'
    ,sum(purchase_scan_num   ) as purchase_scan_num               -- '采购信息浏览量'
    ,sum(suppery_scan_num    ) as suppery_scan_num                -- '供应信息浏览量'
    ,sum(rent_scan_num       ) as rent_scan_num                   -- '车找苗信息浏览量'
    ,sum(lease_scan_num      ) as lease_scan_num                  -- '苗找车信息浏览量'
	,max(part_date)            as part_date                       -- '分区日期'
from db_dw_nstc.clife_nstc_dws_deal_message_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by member_id, release_day


====================================================================================================
==========          clife_nstc_ads_deal_message_info_m                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_deal_message_info_m partition(part_date)
select 
     member_id                                                    -- '会员id'  
    ,substr(release_day,0,7)   as release_month                   -- '发布时间（月）'
    ,max(member_name)          as member_name                     -- '昵称'
    ,sum(purchase_cnt        ) as purchase_cnt                    -- '采购信息数量'
    ,sum(suppery_cnt         ) as suppery_cnt                     -- '供应信息数量'
    ,sum(rent_cnt            ) as rent_cnt                        -- '车找苗信息数量'
    ,sum(lease_cnt           ) as lease_cnt                       -- '苗找车信息数量'
    ,sum(purchase_release_cnt) as purchase_release_cnt            -- '采购信息发布中数量'
    ,sum(suppery_release_cnt ) as suppery_release_cnt             -- '供应信息发布中数量'
    ,sum(rent_release_cnt    ) as rent_release_cnt                -- '车找苗信息发布中数量'
    ,sum(lease_release_cnt   ) as lease_release_cnt               -- '苗找车信息发布中数量'
    ,sum(purchase_finish_cnt ) as purchase_finish_cnt             -- '采购信息已结束数量'
    ,sum(suppery_finish_cnt  ) as suppery_finish_cnt              -- '供应信息已结束数量'
    ,sum(rent_finish_cnt     ) as rent_finish_cnt                 -- '车找苗信息已结束数量'
    ,sum(lease_finish_cnt    ) as lease_finish_cnt                -- '苗找车信息已结束数量'
    ,sum(purchase_return_cnt ) as purchase_return_cnt             -- '采购信息被驳回数量'
    ,sum(suppery_return_cnt  ) as suppery_return_cnt              -- '供应信息被驳回数量'
    ,sum(rent_return_cnt     ) as rent_return_cnt                 -- '车找苗信息被驳回数量'
    ,sum(lease_return_cnt    ) as lease_return_cnt                -- '苗找车信息被驳回数量'
    ,sum(purchase_scan_num   ) as purchase_scan_num               -- '采购信息浏览量'
    ,sum(suppery_scan_num    ) as suppery_scan_num                -- '供应信息浏览量'
    ,sum(rent_scan_num       ) as rent_scan_num                   -- '车找苗信息浏览量'
    ,sum(lease_scan_num      ) as lease_scan_num                  -- '苗找车信息浏览量'
	,max(part_date)            as part_date                       -- '分区日期'
from db_dw_nstc.clife_nstc_dws_deal_message_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by member_id, substr(release_day,0,7)


====================================================================================================
==========          clife_nstc_ads_deal_message_info_y                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_deal_message_info_y partition(part_date)
select 
     member_id                                                    -- '会员id'  
    ,substr(release_day,0,4)   as release_year                    -- '发布时间（年）'
    ,max(member_name)          as member_name                     -- '昵称'
    ,sum(purchase_cnt        ) as purchase_cnt                    -- '采购信息数量'
    ,sum(suppery_cnt         ) as suppery_cnt                     -- '供应信息数量'
    ,sum(rent_cnt            ) as rent_cnt                        -- '车找苗信息数量'
    ,sum(lease_cnt           ) as lease_cnt                       -- '苗找车信息数量'
    ,sum(purchase_release_cnt) as purchase_release_cnt            -- '采购信息发布中数量'
    ,sum(suppery_release_cnt ) as suppery_release_cnt             -- '供应信息发布中数量'
    ,sum(rent_release_cnt    ) as rent_release_cnt                -- '车找苗信息发布中数量'
    ,sum(lease_release_cnt   ) as lease_release_cnt               -- '苗找车信息发布中数量'
    ,sum(purchase_finish_cnt ) as purchase_finish_cnt             -- '采购信息已结束数量'
    ,sum(suppery_finish_cnt  ) as suppery_finish_cnt              -- '供应信息已结束数量'
    ,sum(rent_finish_cnt     ) as rent_finish_cnt                 -- '车找苗信息已结束数量'
    ,sum(lease_finish_cnt    ) as lease_finish_cnt                -- '苗找车信息已结束数量'
    ,sum(purchase_return_cnt ) as purchase_return_cnt             -- '采购信息被驳回数量'
    ,sum(suppery_return_cnt  ) as suppery_return_cnt              -- '供应信息被驳回数量'
    ,sum(rent_return_cnt     ) as rent_return_cnt                 -- '车找苗信息被驳回数量'
    ,sum(lease_return_cnt    ) as lease_return_cnt                -- '苗找车信息被驳回数量'
    ,sum(purchase_scan_num   ) as purchase_scan_num               -- '采购信息浏览量'
    ,sum(suppery_scan_num    ) as suppery_scan_num                -- '供应信息浏览量'
    ,sum(rent_scan_num       ) as rent_scan_num                   -- '车找苗信息浏览量'
    ,sum(lease_scan_num      ) as lease_scan_num                  -- '苗找车信息浏览量'
	,max(part_date)            as part_date                       -- '分区日期'
from db_dw_nstc.clife_nstc_dws_deal_message_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by member_id, substr(release_day,0,4)



====================================================================================================
==========          clife_nstc_ads_order_detail_comment_info_d                            ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_comment_info_d partition(part_date)
select
     product_id                                                                           -- '产品id'
    ,comment_day                                                                          -- '评论日期（日）'
    ,sum(comment_cnt           )                            as  comment_cnt               -- '评价数'
    ,sum(zero_star_comment_cnt )                            as  zero_star_comment_cnt     -- '0星级评价数量'
    ,sum(one_star_comment_cnt  )                            as  one_star_comment_cnt      -- '1星级评价数量'
    ,sum(two_star_comment_cnt  )                            as  two_star_comment_cnt      -- '2星级评价数量'
    ,sum(three_star_comment_cnt)                            as  three_star_comment_cnt    -- '3星级评价数量'
    ,sum(four_star_comment_cnt )                            as  four_star_comment_cnt     -- '4星级评价数量'
    ,sum(five_star_comment_cnt )                            as  five_star_comment_cnt     -- '5星级评价数量'
    ,round(sum(zero_star_comment_cnt ) / sum(comment_cnt),2)  as  zero_star_comment_per     -- '0星级评价数量占比'
    ,round(sum(one_star_comment_cnt  ) / sum(comment_cnt),2)  as  one_star_comment_per      -- '1星级评价数量占比'
    ,round(sum(two_star_comment_cnt  ) / sum(comment_cnt),2)  as  two_star_comment_per      -- '2星级评价数量占比'
    ,round(sum(three_star_comment_cnt) / sum(comment_cnt),2)  as  three_star_comment_per    -- '3星级评价数量占比'
    ,round(sum(four_star_comment_cnt ) / sum(comment_cnt),2)  as  four_star_comment_per     -- '4星级评价数量占比'
    ,round(sum(five_star_comment_cnt ) / sum(comment_cnt),2)  as  five_star_comment_per     -- '5星级评价数量占比'
    ,max(part_date)              as  part_date                 -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_comment_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, comment_day



====================================================================================================
==========          clife_nstc_ads_order_detail_comment_info_y                            ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_comment_info_y partition(part_date)
select
     product_id                                                                           -- '产品id'
    ,substr(comment_day,0,4)                                as  comment_year              -- '评论日期（年）'
    ,sum(comment_cnt           )                            as  comment_cnt               -- '评价数'
    ,sum(zero_star_comment_cnt )                            as  zero_star_comment_cnt     -- '0星级评价数量'
    ,sum(one_star_comment_cnt  )                            as  one_star_comment_cnt      -- '1星级评价数量'
    ,sum(two_star_comment_cnt  )                            as  two_star_comment_cnt      -- '2星级评价数量'
    ,sum(three_star_comment_cnt)                            as  three_star_comment_cnt    -- '3星级评价数量'
    ,sum(four_star_comment_cnt )                            as  four_star_comment_cnt     -- '4星级评价数量'
    ,sum(five_star_comment_cnt )                            as  five_star_comment_cnt     -- '5星级评价数量'
    ,round(sum(zero_star_comment_cnt ) / sum(comment_cnt),2)  as  zero_star_comment_per     -- '0星级评价数量占比'
    ,round(sum(one_star_comment_cnt  ) / sum(comment_cnt),2)  as  one_star_comment_per      -- '1星级评价数量占比'
    ,round(sum(two_star_comment_cnt  ) / sum(comment_cnt),2)  as  two_star_comment_per      -- '2星级评价数量占比'
    ,round(sum(three_star_comment_cnt) / sum(comment_cnt),2)  as  three_star_comment_per    -- '3星级评价数量占比'
    ,round(sum(four_star_comment_cnt ) / sum(comment_cnt),2)  as  four_star_comment_per     -- '4星级评价数量占比'
    ,round(sum(five_star_comment_cnt ) / sum(comment_cnt),2)  as  five_star_comment_per     -- '5星级评价数量占比'
    ,max(part_date)              as  part_date                 -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_comment_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, substr(comment_day,0,4)



====================================================================================================
==========          clife_nstc_ads_order_detail_comment_info_m                            ==========
====================================================================================================

insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_comment_info_m partition(part_date)
select
     product_id                                                                           -- '产品id'
    ,substr(comment_day,0,7)                                as  comment_month             -- '评论日期（月）'
    ,sum(comment_cnt           )                            as  comment_cnt               -- '评价数'
    ,sum(zero_star_comment_cnt )                            as  zero_star_comment_cnt     -- '0星级评价数量'
    ,sum(one_star_comment_cnt  )                            as  one_star_comment_cnt      -- '1星级评价数量'
    ,sum(two_star_comment_cnt  )                            as  two_star_comment_cnt      -- '2星级评价数量'
    ,sum(three_star_comment_cnt)                            as  three_star_comment_cnt    -- '3星级评价数量'
    ,sum(four_star_comment_cnt )                            as  four_star_comment_cnt     -- '4星级评价数量'
    ,sum(five_star_comment_cnt )                            as  five_star_comment_cnt     -- '5星级评价数量'
    ,round(sum(zero_star_comment_cnt ) / sum(comment_cnt),2)  as  zero_star_comment_per     -- '0星级评价数量占比'
    ,round(sum(one_star_comment_cnt  ) / sum(comment_cnt),2)  as  one_star_comment_per      -- '1星级评价数量占比'
    ,round(sum(two_star_comment_cnt  ) / sum(comment_cnt),2)  as  two_star_comment_per      -- '2星级评价数量占比'
    ,round(sum(three_star_comment_cnt) / sum(comment_cnt),2)  as  three_star_comment_per    -- '3星级评价数量占比'
    ,round(sum(four_star_comment_cnt ) / sum(comment_cnt),2)  as  four_star_comment_per     -- '4星级评价数量占比'
    ,round(sum(five_star_comment_cnt ) / sum(comment_cnt),2)  as  five_star_comment_per     -- '5星级评价数量占比'
    ,max(part_date)              as  part_date                 -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_comment_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, substr(comment_day,0,7)


====================================================================================================
==========          clife_nstc_ads_order_detail_info_d                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_info_d partition(part_date)
select
     product_id                                    -- '产品id'
    ,order_detail_day                              -- '订单创建日期（日）'
    ,max(product_name)    as product_name          -- '货品名' 
    ,sum(sale_cnt      )  as sale_cnt              -- '购买次数'
    ,sum(total_cnt     )  as total_cnt             -- '购买总量'
    ,sum(if(total_recom_xq is null, 0, total_recom_xq))  as total_recom_xq        -- '邮费总额'
    ,sum(sku_cnt       )  as sku_cnt               -- 'SKU数'
    ,sum(if(total_price is null, 0, total_price)   )     as total_price           -- '消费总额'
    ,sum(total_back_cnt)  as total_back_cnt        -- '退货数量'
    ,sum(back_cnt      )  as back_cnt              -- '退货次数'
    ,max(part_date)       as part_date             -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, order_detail_day



====================================================================================================
==========          clife_nstc_ads_order_detail_info_m                                    ==========
====================================================================================================

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


====================================================================================================
==========          clife_nstc_ads_order_detail_info_y                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_info_y partition(part_date)
select
     product_id                                              -- '产品id'
    ,substr(order_detail_day,0,4)   as order_detail_year     -- '订单创建日期（年）'
    ,max(product_name)              as product_name          -- '货品名' 
    ,sum(sale_cnt      )            as sale_cnt              -- '购买次数'
    ,sum(total_cnt     )            as total_cnt             -- '购买总量'
    ,sum(if(total_recom_xq is null, 0, total_recom_xq))  as total_recom_xq        -- '邮费总额'
    ,sum(sku_cnt       )            as sku_cnt               -- 'SKU数'
    ,sum(if(total_price is null, 0, total_price)   )     as total_price           -- '消费总额'
    ,sum(total_back_cnt)            as total_back_cnt        -- '退货数量'
    ,sum(back_cnt      )            as back_cnt              -- '退货次数'
    ,max(part_date)                 as part_date             -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, substr(order_detail_day,0,4)



====================================================================================================
==========          clife_nstc_ads_order_by_shop_d                                        ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_by_shop_d partition(part_date)
select
     shop_id                    --店铺id
    ,date_time                  --业务日期
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
      ,date_time                  --业务日期
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
          ,date_time
)t


====================================================================================================
==========          clife_nstc_ads_order_by_shop_m                                        ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_by_shop_m partition(part_date)
select
     shop_id                    --店铺id
    ,month_date                 --业务日期
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
      ,substr(date_time,1,7) as month_date         --业务日期
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
          ,substr(date_time,1,7)
)t


====================================================================================================
==========          clife_nstc_ads_order_by_shop_y                                        ==========
====================================================================================================
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


====================================================================================================
==========          clife_nstc_ads_event_by_object_d                                      ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_event_by_object_d partition(part_date)
select 
     object_id     --被操作对象id
    ,date_time     --业务日期
    ,max(object_type)        as  object_type  --对象类型
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(browse_num)    as browse_num    --浏览量
    ,sum(attention_num) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dws_event_converge
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  object_id
         ,date_time


====================================================================================================
==========          clife_nstc_ads_event_by_object_m                                      ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_event_by_object_m partition(part_date)
select 
     object_id     --被操作对象id
    ,substr(date_time,1,7) as month_date     --业务日期
    ,max(object_type)        as  object_type  --对象类型
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(browse_num)    as browse_num    --浏览量
    ,sum(attention_num) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dws_event_converge
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  object_id
         ,substr(date_time,1,7)


====================================================================================================
==========          clife_nstc_ads_event_by_object_y                                      ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_event_by_object_y partition(part_date)
select 
     object_id     --被操作对象id
    ,substr(date_time,1,4) as year_date     --业务日期
    ,max(object_type)        as  object_type  --对象类型
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(browse_num)    as browse_num    --浏览量
    ,sum(attention_num) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dws_event_converge
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  object_id
         ,substr(date_time,1,4)


====================================================================================================
==========          clife_nstc_ads_order_detail_info_f                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_info_f partition(part_date)
select
     sum(sale_cnt)        as sale_cnt              -- '购买次数'
    ,sum(total_cnt)       as total_cnt             -- '购买总量'
    ,sum(if(total_recom_xq is null, 0, total_recom_xq))  as total_recom_xq        -- '邮费总额'
    ,sum(sku_cnt )        as sku_cnt               -- 'SKU数'
    ,sum(if(total_price is null, 0, total_price)   )     as total_price           -- '消费总额'
    ,sum(total_back_cnt)  as total_back_cnt        -- '退货数量'
    ,sum(back_cnt)        as back_cnt              -- '退货次数'
    ,max(part_date)       as part_date             -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 


====================================================================================================
==========          clife_nstc_ads_order_detail_comment_info_f                            ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_comment_info_f partition(part_date)
select
     comment_cnt               -- '评价数'
    ,zero_star_comment_cnt     -- '0星级评价数量'
    ,one_star_comment_cnt      -- '1星级评价数量'
    ,two_star_comment_cnt      -- '2星级评价数量'
    ,three_star_comment_cnt    -- '3星级评价数量'
    ,four_star_comment_cnt     -- '4星级评价数量'
    ,five_star_comment_cnt     -- '5星级评价数量'
    ,cast(coalesce(zero_star_comment_cnt/comment_cnt,0)  as decimal(5,2))  as  zero_star_comment_per     -- '0星级评价数量占比'
    ,cast(coalesce(one_star_comment_cnt/comment_cnt,0)   as decimal(5,2))  as  one_star_comment_per      -- '1星级评价数量占比'
    ,cast(coalesce(two_star_comment_cnt/comment_cnt,0)   as decimal(5,2))  as  two_star_comment_per      -- '2星级评价数量占比'
    ,cast(coalesce(three_star_comment_cnt/comment_cnt,0) as decimal(5,2))  as  three_star_comment_per    -- '3星级评价数量占比'
    ,cast(coalesce(four_star_comment_cnt/comment_cnt,0)  as decimal(5,2))  as  four_star_comment_per     -- '4星级评价数量占比'
    ,cast(coalesce(five_star_comment_cnt/comment_cnt,0)  as decimal(5,2))  as  five_star_comment_per     -- '5星级评价数量占比'
    ,part_date                 -- '分区日期'
from
(
  select
       sum(comment_cnt           )                            as  comment_cnt               -- '评价数'
      ,sum(zero_star_comment_cnt )                            as  zero_star_comment_cnt     -- '0星级评价数量'
      ,sum(one_star_comment_cnt  )                            as  one_star_comment_cnt      -- '1星级评价数量'
      ,sum(two_star_comment_cnt  )                            as  two_star_comment_cnt      -- '2星级评价数量'
      ,sum(three_star_comment_cnt)                            as  three_star_comment_cnt    -- '3星级评价数量'
      ,sum(four_star_comment_cnt )                            as  four_star_comment_cnt     -- '4星级评价数量'
      ,sum(five_star_comment_cnt )                            as  five_star_comment_cnt     -- '5星级评价数量'
      ,max(part_date)              as  part_date
  from db_dw_nstc.clife_nstc_dws_order_detail_comment_info
  where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
)t


====================================================================================================
==========          clife_nstc_ads_deal_message_info_f                                    ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_deal_message_info_f partition(part_date)
select 
     sum(purchase_cnt        ) as purchase_cnt                    -- '采购信息数量'
    ,sum(suppery_cnt         ) as suppery_cnt                     -- '供应信息数量'
    ,sum(rent_cnt            ) as rent_cnt                        -- '车找苗信息数量'
    ,sum(lease_cnt           ) as lease_cnt                       -- '苗找车信息数量'
    ,sum(purchase_release_cnt) as purchase_release_cnt            -- '采购信息发布中数量'
    ,sum(suppery_release_cnt ) as suppery_release_cnt             -- '供应信息发布中数量'
    ,sum(rent_release_cnt    ) as rent_release_cnt                -- '车找苗信息发布中数量'
    ,sum(lease_release_cnt   ) as lease_release_cnt               -- '苗找车信息发布中数量'
    ,sum(purchase_finish_cnt ) as purchase_finish_cnt             -- '采购信息已结束数量'
    ,sum(suppery_finish_cnt  ) as suppery_finish_cnt              -- '供应信息已结束数量'
    ,sum(rent_finish_cnt     ) as rent_finish_cnt                 -- '车找苗信息已结束数量'
    ,sum(lease_finish_cnt    ) as lease_finish_cnt                -- '苗找车信息已结束数量'
    ,sum(purchase_return_cnt ) as purchase_return_cnt             -- '采购信息被驳回数量'
    ,sum(suppery_return_cnt  ) as suppery_return_cnt              -- '供应信息被驳回数量'
    ,sum(rent_return_cnt     ) as rent_return_cnt                 -- '车找苗信息被驳回数量'
    ,sum(lease_return_cnt    ) as lease_return_cnt                -- '苗找车信息被驳回数量'
    ,sum(purchase_scan_num   ) as purchase_scan_num               -- '采购信息浏览量'
    ,sum(suppery_scan_num    ) as suppery_scan_num                -- '供应信息浏览量'
    ,sum(rent_scan_num       ) as rent_scan_num                   -- '车找苗信息浏览量'
    ,sum(lease_scan_num      ) as lease_scan_num                  -- '苗找车信息浏览量'
	,max(part_date)            as part_date                       -- '分区日期'
from db_dw_nstc.clife_nstc_dws_deal_message_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 


====================================================================================================
==========          clife_nstc_ads_event_by_object_f                                      ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_event_by_object_f partition(part_date)
select 
     object_id     --被操作对象id
    ,max(object_type)        as  object_type  --对象类型
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(browse_num)    as browse_num    --浏览量
    ,sum(attention_num) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dws_event_converge
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  object_id


====================================================================================================
==========          clife_nstc_ads_order_f                                                ==========
====================================================================================================
insert overwrite table db_ads_nstc.clife_nstc_ads_order_f partition(part_date)
select
     coalesce(bug_product_num,0)           as bug_product_num           --产品购买量
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
       sum(bug_product_num)            as bug_product_num          --产品购买量
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
)t


