====================================================================================================
==========          db_dw_nstc.clife_nstc_dws_order_detail_comment_info                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dws_order_detail_comment_info ( 
     product_id                     string               comment '产品id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '评论人id' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,comment_day                    string               comment '评论日期' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
) comment '用户评价明细汇聚表dws' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dws_order_detail_info                           ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dws_order_detail_info ( 
     member_id                      string               comment '买家id' 
    ,product_id                     string               comment '产品id' 
    ,goods_id                       string               comment '商品id' 
    ,class_id                       string               comment '商品分类' 
    ,type_id                        string               comment '商品类型' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,order_detail_day               string               comment '订单创建日期' 
    ,class_name                     string               comment '商品分类名称' 
    ,type_name                      string               comment '商品类型名称' 
    ,shop_id                        string               comment '店铺id' 
    ,product_name                   string               comment '货品名' 
    ,sale_cnt                       bigint               comment '购买次数' 
    ,total_cnt                      bigint               comment '购买总量' 
    ,total_recom_xq                 bigint               comment '邮费总额' 
    ,sku_cnt                        bigint               comment 'SKU数' 
    ,total_price                    double               comment '消费总额' 
    ,total_back_cnt                 bigint               comment '退货数量' 
    ,back_cnt                       bigint               comment '退货次数' 
) comment '产品交易明细事实表dws' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dws_order_converge                              ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dws_order_converge ( 
     member_id                      string               comment '会员ID' 
    ,shop_id                        string               comment '店铺id' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,date_time                      string               comment '业务日期' 
    ,member_name                    string               comment '会员名称' 
    ,member_identity                int                  comment '身份类型' 
    ,shop_name                      string               comment '店铺名称' 
    ,shop_main                      string               comment '店铺主营品种' 
    ,shop_address                   string               comment '店铺地址' 
    ,bug_product_num                int                  comment '产品购买量' 
    ,bug_goods_num                  int                  comment '商品够买量' 
    ,order_num                      int                  comment '下单总数' 
    ,discuss_order_num              int                  comment '商议订单数' 
    ,wait_pay_order_num             int                  comment '待支付订单数' 
    ,wait_delivery_order_num        int                  comment '待发货订单数' 
    ,wait_receiv_order_num          int                  comment '待收货订单数' 
    ,wait_evaluation_order_num      int                  comment '待评价订单数' 
    ,return_order_o_num             int                  comment '退货中订单数' 
    ,return_order_s_num             int                  comment '退货成功订单数' 
    ,complete_order_num             int                  comment '订单完成订单数' 
    ,complete_cancel_num            int                  comment '订单取消订单数' 
    ,order_price_all                double               comment '订单总额' 
    ,sub_money_all                  double               comment '优惠金额' 
    ,payment                        double               comment '实付金额' 
    ,pay_order_num                  int                  comment '支付订单总数' 
    ,no_pay_order_num               int                  comment '未支付订单数' 
    ,wechar_pay_order_num           int                  comment '微信支付订单数' 
    ,alipay_pay_order_num           int                  comment '支付宝支付订单数' 
    ,app_pay_order_num              int                  comment '小程序支付订单数' 
    ,icbc_app_pay_num               int                  comment '工行app支付订单数' 
    ,icbc_pc_pay_num                int                  comment '工行pc支付订单数' 
    ,order_post                     double               comment '邮费总额' 
    ,shipping_order_num             int                  comment '发货订单数' 
    ,company_shipping_num           int                  comment '发公司物流类型订单数' 
    ,personal_shipping_num          int                  comment '发个人物流类型订单数' 
    ,no_remind_shipping_num         int                  comment '发货未提醒数' 
    ,remind_shipping_num            int                  comment '发货提醒数' 
    ,receiving_order_num            int                  comment '收货订单数' 
    ,appeal_order_num               int                  comment '申诉订单数' 
    ,no_deal_appeal_num             int                  comment '申诉未处理订单数' 
    ,deal_appeal_order_num          int                  comment '申诉已处理订单数' 
    ,return_order_num               int                  comment '退货订单总数' 
    ,back_price_all                 double               comment '退款总金额' 
    ,unshipped_refunded_num         int                  comment '未发货退款订单数' 
    ,shipped_no_received_num        int                  comment '已发货未收到货退款订单数' 
    ,received_back_num              int                  comment '收到货仅退款订单数' 
    ,received_pay_back_num          int                  comment '收到货退货退款订单数' 
    ,company_back_num               int                  comment '退货公司物流订单数' 
    ,personal_back_num              int                  comment '退货个人找车订单数' 
    ,inreview_back_num              int                  comment '退货审核中订单数' 
    ,agree_back_num                 int                  comment '退货同意订单数' 
    ,refuse_back_num                int                  comment '退货拒绝订单数' 
    ,evaluation_order_num           int                  comment '完成评价数' 
    ,no_evaluation_num              int                  comment '未完成评价数' 
    ,deal_complete_back_num         int                  comment '退货订单交易完成数' 
    ,deal_complete_no_back_num      int                  comment '没有退货订单交易完成数' 
    ,deal_complete_num              int                  comment '交易完成订单数' 
) comment '订单信息多事物汇聚表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dws_event_converge                              ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dws_event_converge ( 
     member_id                      string               comment '会员id' 
    ,object_id                      string               comment '被操作对象id' 
    ,date_time                      string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,member_name                    string               comment '会员名称' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '用户行为汇聚表事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dws_deal_message_info                           ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dws_deal_message_info ( 
     member_id                      string               comment '会员id' 
    ,goods_class                    string               comment '种类' 
    ,start_place                    string               comment '开始地' 
    ,end_place                      string               comment '目的地' 
    ,release_day                    string               comment '发布时间' 
    ,member_name                    string               comment '昵称' 
    ,purchase_cnt                   bigint               comment '采购信息数量' 
    ,suppery_cnt                    bigint               comment '供应信息数量' 
    ,rent_cnt                       bigint               comment '车找苗信息数量' 
    ,lease_cnt                      bigint               comment '苗找车信息数量' 
    ,purchase_release_cnt           bigint               comment '采购信息发布中数量' 
    ,suppery_release_cnt            bigint               comment '供应信息发布中数量' 
    ,rent_release_cnt               bigint               comment '车找苗信息发布中数量' 
    ,lease_release_cnt              bigint               comment '苗找车信息发布中数量' 
    ,purchase_finish_cnt            bigint               comment '采购信息已结束数量' 
    ,suppery_finish_cnt             bigint               comment '供应信息已结束数量' 
    ,rent_finish_cnt                bigint               comment '车找苗信息已结束数量' 
    ,lease_finish_cnt               bigint               comment '苗找车信息已结束数量' 
    ,purchase_return_cnt            bigint               comment '采购信息被驳回数量' 
    ,suppery_return_cnt             bigint               comment '供应信息被驳回数量' 
    ,rent_return_cnt                bigint               comment '车找苗信息被驳回数量' 
    ,lease_return_cnt               bigint               comment '苗找车信息被驳回数量' 
    ,purchase_scan_num              bigint               comment '采购信息浏览量' 
    ,suppery_scan_num               bigint               comment '供应信息浏览量' 
    ,rent_scan_num                  bigint               comment '车找苗信息浏览量' 
    ,lease_scan_num                 bigint               comment '苗找车信息浏览量' 
) comment '买卖信息明细汇聚表dws' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


