====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_deal_message_info_d                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_deal_message_info_d ( 
     member_id                      string               comment '会员id' 
    ,release_day                    string               comment '发布时间（日）' 
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
) comment '会员买卖信息日应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_deal_message_info_m                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_deal_message_info_m ( 
     member_id                      string               comment '会员id' 
    ,release_month                  string               comment '发布时间（月）' 
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
) comment '会员买卖信息月应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_deal_message_info_y                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_deal_message_info_y ( 
     member_id                      string               comment '会员id' 
    ,release_year                   string               comment '发布时间（年）' 
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
) comment '会员买卖信息年应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_info_d                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_info_d ( 
     product_id                     string               comment '产品id' 
    ,order_detail_day               string               comment '订单创建日期（日）' 
    ,product_name                   string               comment '货品名' 
    ,sale_cnt                       bigint               comment '购买次数' 
    ,total_cnt                      bigint               comment '购买总量' 
    ,total_recom_xq                 bigint               comment '邮费总额' 
    ,sku_cnt                        bigint               comment 'SKU数' 
    ,total_price                    double               comment '消费总额' 
    ,total_back_cnt                 bigint               comment '退货数量' 
    ,back_cnt                       bigint               comment '退货次数' 
) comment '产品交易信息日应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_info_m                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_info_m ( 
     product_id                     string               comment '产品id' 
    ,order_detail_month             string               comment '订单创建日期（月）' 
    ,product_name                   string               comment '货品名' 
    ,sale_cnt                       bigint               comment '购买次数' 
    ,total_cnt                      bigint               comment '购买总量' 
    ,total_recom_xq                 bigint               comment '邮费总额' 
    ,sku_cnt                        bigint               comment 'SKU数' 
    ,total_price                    double               comment '消费总额' 
    ,total_back_cnt                 bigint               comment '退货数量' 
    ,back_cnt                       bigint               comment '退货次数' 
) comment '产品交易信息月应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_info_y                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_info_y ( 
     product_id                     string               comment '产品id' 
    ,order_detail_year              string               comment '订单创建日期（年）' 
    ,product_name                   string               comment '货品名' 
    ,sale_cnt                       bigint               comment '购买次数' 
    ,total_cnt                      bigint               comment '购买总量' 
    ,total_recom_xq                 bigint               comment '邮费总额' 
    ,sku_cnt                        bigint               comment 'SKU数' 
    ,total_price                    double               comment '消费总额' 
    ,total_back_cnt                 bigint               comment '退货数量' 
    ,back_cnt                       bigint               comment '退货次数' 
) comment '产品交易信息年应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_event_by_object_d                          ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_event_by_object_d ( 
     object_id                      string               comment '被操作对象id' 
    ,date_time                      string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '商品/店铺被浏览关注日应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_event_by_object_m                          ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_event_by_object_m ( 
     object_id                      string               comment '被操作对象id' 
    ,month_date                     string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '商品/店铺被浏览关注月应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_event_by_object_y                          ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_event_by_object_y ( 
     object_id                      string               comment '被操作对象id' 
    ,year_date                      string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '商品/店铺被浏览关注年应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_by_shop_d                            ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_by_shop_d ( 
     shop_id                        string               comment '店铺id' 
    ,date_time                      string               comment '业务日期' 
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
    ,evaluation_rate                double               comment '订单完成评价类型数量占比' 
    ,no_evaluation_rate             double               comment '订单未评价类型数量占比' 
) comment '店铺订单信息月应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_by_shop_m                            ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_by_shop_m ( 
     shop_id                        string               comment '店铺id' 
    ,month_date                     string               comment '业务日期' 
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
    ,evaluation_rate                double               comment '订单完成评价类型数量占比' 
    ,no_evaluation_rate             double               comment '订单未评价类型数量占比' 
) comment '店铺订单信息日应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_by_shop_y                            ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_by_shop_y ( 
     shop_id                        string               comment '店铺id' 
    ,year_date                      string               comment '业务日期' 
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
    ,evaluation_rate                double               comment '订单完成评价类型数量占比' 
    ,no_evaluation_rate             double               comment '订单未评价类型数量占比' 
) comment '店铺订单信息年应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_comment_info_d                ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_comment_info_d ( 
     product_id                     string               comment '产品id' 
    ,comment_day                    string               comment '评论日期（日）' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
    ,zero_star_comment_per          double               comment '0星级评价数量占比' 
    ,one_star_comment_per           double               comment '1星级评价数量占比' 
    ,two_star_comment_per           double               comment '2星级评价数量占比' 
    ,three_star_comment_per         double               comment '3星级评价数量占比' 
    ,four_star_comment_per          double               comment '4星级评价数量占比' 
    ,five_star_comment_per          double               comment '5星级评价数量占比' 
) comment '产品评价信息日应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_comment_info_m                ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_comment_info_m ( 
     product_id                     string               comment '产品id' 
    ,comment_month                  string               comment '评论日期（月）' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
    ,zero_star_comment_per          double               comment '0星级评价数量占比' 
    ,one_star_comment_per           double               comment '1星级评价数量占比' 
    ,two_star_comment_per           double               comment '2星级评价数量占比' 
    ,three_star_comment_per         double               comment '3星级评价数量占比' 
    ,four_star_comment_per          double               comment '4星级评价数量占比' 
    ,five_star_comment_per          double               comment '5星级评价数量占比' 
) comment '产品评价信息月应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_comment_info_y                ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_comment_info_y ( 
     product_id                     string               comment '产品id' 
    ,comment_year                   string               comment '评论日期（年）' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
    ,zero_star_comment_per          double               comment '0星级评价数量占比' 
    ,one_star_comment_per           double               comment '1星级评价数量占比' 
    ,two_star_comment_per           double               comment '2星级评价数量占比' 
    ,three_star_comment_per         double               comment '3星级评价数量占比' 
    ,four_star_comment_per          double               comment '4星级评价数量占比' 
    ,five_star_comment_per          double               comment '5星级评价数量占比' 
) comment '产品评价信息年应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_info_f                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_info_f ( 
     sale_cnt                       int                  comment '购买次数' 
    ,total_cnt                      int                  comment '购买总量' 
    ,total_recom_xq                 int                  comment '邮费总额' 
    ,sku_cnt                        int                  comment 'SKU数' 
    ,total_price                    int                  comment '消费总额' 
    ,total_back_cnt                 int                  comment '退货数量' 
    ,back_cnt                       int                  comment '退货次数' 
) comment '交易信息累计应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_detail_comment_info_f                ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_comment_info_f ( 
     comment_cnt                    int                  comment '评价数' 
    ,zero_star_comment_cnt          int                  comment '0星级评价数量' 
    ,one_star_comment_cnt           int                  comment '1星级评价数量' 
    ,two_star_comment_cnt           int                  comment '2星级评价数量' 
    ,three_star_comment_cnt         int                  comment '3星级评价数量' 
    ,four_star_comment_cnt          int                  comment '4星级评价数量' 
    ,five_star_comment_cnt          int                  comment '5星级评价数量' 
    ,zero_star_comment_per          double               comment '0星级评价数量占比' 
    ,one_star_comment_per           double               comment '1星级评价数量占比' 
    ,two_star_comment_per           double               comment '2星级评价数量占比' 
    ,three_star_comment_per         double               comment '3星级评价数量占比' 
    ,four_star_comment_per          double               comment '4星级评价数量占比' 
    ,five_star_comment_per          double               comment '5星级评价数量占比' 
) comment '产品评价信息累计应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_deal_message_info_f                        ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_deal_message_info_f ( 
     purchase_cnt                   int                  comment '采购信息数量' 
    ,suppery_cnt                    int                  comment '供应信息数量' 
    ,rent_cnt                       int                  comment '车找苗信息数量' 
    ,lease_cnt                      int                  comment '苗找车信息数量' 
    ,purchase_release_cnt           int                  comment '采购信息发布中数量' 
    ,suppery_release_cnt            int                  comment '供应信息发布中数量' 
    ,rent_release_cnt               int                  comment '车找苗信息发布中数量' 
    ,lease_release_cnt              int                  comment '苗找车信息发布中数量' 
    ,purchase_finish_cnt            int                  comment '采购信息已结束数量' 
    ,suppery_finish_cnt             int                  comment '供应信息已结束数量' 
    ,rent_finish_cnt                int                  comment '车找苗信息已结束数量' 
    ,lease_finish_cnt               int                  comment '苗找车信息已结束数量' 
    ,purchase_return_cnt            int                  comment '采购信息被驳回数量' 
    ,suppery_return_cnt             int                  comment '供应信息被驳回数量' 
    ,rent_return_cnt                int                  comment '车找苗信息被驳回数量' 
    ,lease_return_cnt               int                  comment '苗找车信息被驳回数量' 
    ,purchase_scan_num              int                  comment '采购信息浏览量' 
    ,suppery_scan_num               int                  comment '供应信息浏览量' 
    ,rent_scan_num                  int                  comment '车找苗信息浏览量' 
    ,lease_scan_num                 int                  comment '苗找车信息浏览量' 
) comment '买卖信息累计应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_event_by_object_f                          ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_event_by_object_f ( 
     object_id                      string               comment '被操作对象id' 
    ,object_type                    int                  comment '对象类型' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '商品/店铺被浏览关注累计应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ads_nstc.clife_nstc_ads_order_f                                    ==========
====================================================================================================
create table if not exists db_ads_nstc.clife_nstc_ads_order_f ( 
     bug_product_num                int                  comment '产品购买量' 
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
    ,evaluation_rate                double               comment '订单完成评价类型数量占比' 
    ,no_evaluation_rate             double               comment '订单未评价类型数量占比' 
) comment '订单信息累计应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


