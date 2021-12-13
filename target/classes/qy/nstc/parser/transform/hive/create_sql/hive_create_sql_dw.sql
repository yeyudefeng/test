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
==========          db_dw_nstc.clife_nstc_dim_date                                        ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_date ( 
     dt_code                        string               comment '日期代码' 
    ,date_s                         string               comment '日期代码/' 
    ,date_w                         string               comment '日期代码-' 
    ,date_cn                        string               comment '日期代码-CN' 
    ,y_code                         string               comment '年份代码' 
    ,q_code                         string               comment '季度代码' 
    ,q_code_cn                      string               comment '季度-CN' 
    ,m_code                         string               comment '月份代码' 
    ,m_code_cn                      string               comment '月份-CN' 
    ,week_of_month_cn               string               comment '本月第几周' 
    ,week_of_year_cn                string               comment '本年第几周' 
    ,d_code                         string               comment '日' 
    ,day_of_year_cn                 string               comment '本年第几天' 
    ,week_cn                        string               comment '星期几' 
) comment '时间维表' 
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
==========          db_dw_nstc.clife_nstc_dwd_event_info                                  ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dwd_event_info ( 
     id                             bigint               comment '记录id' 
    ,member_id                      string               comment '会员id' 
    ,member_name                    string               comment '会员名称' 
    ,object_id                      string               comment '被操作对象id （商品 店铺）' 
    ,object_name                    string               comment '被操作对象名称' 
    ,object_type                    string               comment '对象类型' 
    ,behavior_type                  int                  comment '行为动作类型' 
    ,`type`                         int                  comment '是否是新客户' 
    ,create_at                      string               comment '行为时间' 
) comment '用户行为明细事实表' 
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
==========          db_dw_nstc.clife_nstc_dim_shop_user                                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,disabled                       int                  comment '是否冻结账号(0-正常' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商家账号维表' 
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


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_township_area                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_township_area ( 
     id                             int                  comment 'id' 
    ,name_prov                      string               comment '省' 
    ,code_prov                      string               comment '省code' 
    ,name_city                      string               comment '市' 
    ,code_city                      string               comment '市code' 
    ,name_coun                      string               comment '区' 
    ,code_coun                      string               comment '区code' 
    ,name_town                      string               comment '镇' 
    ,code_town                      string               comment '镇code' 
) comment '省市区镇' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_shop_user                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`password`                     string               comment '密码' 
    ,token                          string               comment 'token' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,aurora_id                      string               comment '极光推送id' 
    ,disabled                       tinyint              comment '是否冻结账号(0-正常' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商家账号表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_shop_shop                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_shop ( 
     shop_id                        string               comment '店铺信息id' 
    ,shop_main                      string               comment '主营品种' 
    ,shop_user_id                   string               comment '商户ID' 
    ,shop_name                      string               comment '店铺名称' 
    ,shop_logo                      string               comment '店铺图片' 
    ,shop_background                string               comment '背景图' 
    ,shop_desc                      string               comment '店铺简介' 
    ,shop_notice                    string               comment '店铺介绍内容' 
    ,shop_type                      int                  comment '认证类型0-个人，1-企业，2-经纪人' 
    ,shop_phone                     string               comment '店铺手机号' 
    ,shop_province                  string               comment '店铺所在省' 
    ,shop_city                      string               comment '店铺所在城市' 
    ,shop_area                      string               comment '店铺所在区域' 
    ,shop_town                      string               comment '乡镇' 
    ,shop_address                   string               comment '经营地址' 
    ,shop_sale                      int                  comment '总销量' 
    ,shop_star                      double               comment '信誉值(默认80)' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,recommend                      int                  comment '首页展示0-不展示' 
    ,hits                           int                  comment '浏览量' 
    ,shop_check                     int                  comment '审核0-审核中' 
    ,money                          double               comment '账户余额' 
    ,freeze                         double               comment '冻结金额' 
    ,all_money                      double               comment '总成交额' 
    ,bank_card                      string               comment '银行卡号' 
    ,bank_user_name                 string               comment '持卡人姓名' 
    ,bank_name                      string               comment '银行名字' 
    ,bank_address                   string               comment '开户行地址' 
    ,intro_user                     string               comment '推荐人' 
    ,ali_number                     string               comment '支付宝账号' 
    ,ali_name                       string               comment '支付宝名字' 
    ,times                          int                  comment '发货时间' 
    ,location                       int                  comment '排序' 
    ,create_id                      string               comment '操作人' 
    ,create_at                      string               comment '创建时间' 
    ,disabled                       int                  comment '是否禁用(0正常)' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,batchId                        int                  comment '没有注释' 
) comment '店铺' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_shop_coupon                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_coupon ( 
     id                             string               comment 'ID' 
    ,shop_id                        string               comment '店铺id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,`type`                         int                  comment '优惠券类型(1-满减)' 
    ,sub_money                      double               comment '优惠金额' 
    ,enough_money                   double               comment '满减满足金额' 
    ,total_num                      int                  comment '总数量' 
    ,send_num                       int                  comment '已发数量' 
    ,limit_sartAt                   string               comment '使用期限起' 
    ,limit_endAt                    string               comment '使用期限至' 
    ,limit_number                   int                  comment '允许单人领取数量' 
    ,has_score                      int                  comment '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      double               comment '所需余额' 
    ,`status`                       int                  comment '状态(0可用' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '全局优惠券' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_record_shop_member                         ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_record_shop_member ( 
     id                             bigint               comment '店铺访问记录' 
    ,member_id                      string               comment '会员id' 
    ,shop_id                        string               comment '店铺id' 
    ,`type`                         int                  comment '新老访客0-新访客' 
    ,create_at                      string               comment '访问时间' 
    ,create_day                     string               comment '访问日期' 
) comment '店铺访问记录' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_record_goods_member                        ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_record_goods_member ( 
     id                             bigint               comment '访问记录id' 
    ,member_id                      string               comment '会员id' 
    ,goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,`type`                         int                  comment '新老访客数0-新访客' 
    ,create_at                      string               comment '访问时间' 
    ,create_day                     string               comment '访问日期' 
) comment '商品访问记录' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_qa_question                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_qa_question ( 
     question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '会员id' 
    ,question_content               string               comment '问题描述' 
    ,question_img                   string               comment '图片链接' 
    ,`status`                       int                  comment '显示状态0-显示' 
    ,like_num                       int                  comment '点赞数' 
    ,create_at                      string               comment '发布时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-问题表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_qa_answer                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_qa_answer ( 
     answer_id                      bigint               comment '答案id' 
    ,question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '回答人id' 
    ,answer_content                 string               comment '回答内容' 
    ,`status`                       int                  comment '显示状态0-显示' 
    ,like_num                       int                  comment '回复点赞数' 
    ,create_at                      string               comment '回答时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-答案表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_shipping                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_shipping ( 
     shipping_id                    string               comment '订单收货信息id' 
    ,order_id                       string               comment '订单ID' 
    ,receiver_name                  string               comment '收件人' 
    ,receiver_phone                 string               comment '电话' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,receiver_address               string               comment '地址' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '订单配送表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_order                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_order ( 
     order_id                       string               comment '订单id' 
    ,shop_id                        string               comment '店铺id' 
    ,member_id                      string               comment '会员ID' 
    ,order_price                    double               comment '订单总额' 
    ,payment                        double               comment '实付金额' 
    ,order_sn                       string               comment '订单号' 
    ,merge_sn                       string               comment '合单号' 
    ,order_post                     double               comment '邮费' 
    ,coupon_id                      string               comment '优惠券id' 
    ,`status`                       int                  comment '订单状态(0-商议订单，1：待支付，2：待发货，3：待收货，4-待评价，5：退货中，6.退货成功，7：订单完成，8：订单取消）' 
    ,create_at                      string               comment '下单时间' 
    ,update_at                      string               comment '订单更新时间' 
    ,return_at                      string               comment '退货时间' 
    ,pay_at                         string               comment '支付时间' 
    ,pay_type                       int                  comment '0-默认未支付1：微信支付，2：支付宝支付 ' 
    ,consign_at                     string               comment '发货时间' 
    ,receiving_at                   string               comment '收货时间' 
    ,end_at                         string               comment '交易完成时间' 
    ,close_at                       string               comment '交易关闭时间' 
    ,coment                         int                  comment '是否评价 0：未评价，1：已评价' 
    ,shipping_type                  string               comment '物流类型0-公司' 
    ,shipping_name                  string               comment '物流名称' 
    ,shipping_code                  string               comment '物流单号' 
    ,shipping_reminder              string               comment '发货提醒0-未提醒' 
    ,`read`                         int                  comment '0未读 1已读' 
    ,buyer_msg                      string               comment '买家留言' 
    ,buyer_nick                     string               comment '买家昵称' 
    ,`export`                       int                  comment '是否已经导出  0：未导出， 1：已导出' 
    ,del_flag                       int                  comment '删除标记' 
) comment '订单信息表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_detail                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_detail ( 
     detail_id                      string               comment '订单详情id' 
    ,order_id                       string               comment '订单ID' 
    ,goods_id                       string               comment '商品ID' 
    ,product_id                     string               comment '产品规格ID' 
    ,order_sn                       string               comment '子订单号' 
    ,total                          int                  comment '购买数量' 
    ,coment                         int                  comment '0未评价  1已评价' 
    ,price                          double               comment '单价' 
    ,all_price                      double               comment '总价' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '订单详情表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_coment                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_coment ( 
     comment_id                     string               comment '评论表id' 
    ,detail_id                      string               comment '订单详情表id' 
    ,goods_id                       string               comment '商品id' 
    ,product_id                     string               comment '产品id' 
    ,member_id                      string               comment '评论人' 
    ,star                           int                  comment '评价星级 1-5' 
    ,content                        string               comment '评价内容' 
    ,comment_img                    string               comment '评论图片' 
    ,disply                         int                  comment '删除标记 0：未删除，1：删除' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '评价表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_back                                 ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_back ( 
     back_id                        string               comment '退换货id' 
    ,order_id                       string               comment '订单ID' 
    ,price                          double               comment '退款金额' 
    ,express_name                   string               comment '物流名/司机电话' 
    ,express_code                   string               comment '快物流单号/车牌号' 
    ,`type`                         int                  comment '类型0-公司物流' 
    ,`status`                       int                  comment '状态0-审核中' 
    ,order_status                   int                  comment '订单原状态' 
    ,reason                         string               comment '退货原因' 
    ,back_type                      int                  comment '0-未发货退款、1-已发货未收到货退款、2-收到货仅退款、3-收到货退货退款' 
    ,reason_f                       string               comment '驳回原因' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '退货记录表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_order_appeal                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_appeal ( 
     id                             int                  comment '申诉id' 
    ,order_id                       string               comment '订单id' 
    ,reason_content                 string               comment '申诉原因' 
    ,reason_img                     string               comment '申诉图片' 
    ,`status`                       string               comment '处理状态0-未处理' 
    ,create_at                      string               comment '申诉时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '申诉表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_user                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_user ( 
     member_id                      string               comment '会员id' 
    ,member_phone                   string               comment '手机号' 
    ,member_name                    string               comment '昵称' 
    ,member_avator                  string               comment '头像' 
    ,`password`                     string               comment '密码' 
    ,token                          string               comment 'token' 
    ,`status`                       int                  comment '工行E企付状态0-未提交' 
    ,intro_user                     string               comment '推荐人' 
    ,`level`                        int                  comment '身份0-个人' 
    ,money                          double               comment '余额' 
    ,freeze                         double               comment '数字金额' 
    ,aurora_id                      string               comment '极光推送id' 
    ,create_at                      string               comment '注册时间' 
    ,update_at                      string               comment '更新时间' 
    ,unionid                        string               comment '微信unionid' 
    ,openid                         string               comment '微信openId' 
    ,xcxopenid                      string               comment '小程序openid' 
    ,wx_name                        string               comment '微信昵称' 
    ,ali_name                       string               comment '阿里名字' 
    ,ali_account                    string               comment '阿里手机号' 
    ,disabled                       tinyint              comment '是否冻结账号(0-正常' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,icbc_status                    int                  comment '工行会员认证0-未认证' 
) comment '会员表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_shop                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_shop ( 
     id                             bigint               comment '主键id' 
    ,shop_id                        string               comment '店铺id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '关注时间' 
) comment '店铺关注表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_question                            ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_question ( 
     member_id                      string               comment '会员id' 
    ,question_id                    bigint               comment '问题id' 
) comment '问题点赞表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_follow                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_follow ( 
     follow_id                      bigint               comment '主键id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '创建时间' 
) comment '商品关注表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_coupon                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_coupon ( 
     id                             string               comment 'ID' 
    ,member_id                      string               comment '会员ID' 
    ,coupon_id                      string               comment '优惠券ID' 
    ,`type`                         int                  comment '优惠券类型（1 满减 ）' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,sub_money                      double               comment '优惠金额' 
    ,coupon_money                   double               comment '满减满足金额' 
    ,order_id                       string               comment '订单ID' 
    ,`status`                       int                  comment '优惠券状态（0-未使用，1-已使用，2-已过期，3-已失效）' 
    ,create_at                      string               comment '获取时间' 
    ,order_at                       string               comment '使用时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '用户优惠券' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_answer                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_answer ( 
     member_id                      string               comment '会员id' 
    ,answer_id                      bigint               comment '回复id' 
) comment '回复点赞表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_address                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_address ( 
     address_id                     int                  comment '收货地址id' 
    ,member_id                      string               comment '会员ID' 
    ,province                       string               comment '所在省' 
    ,city                           string               comment '所在市' 
    ,area                           string               comment '所在县区' 
    ,town                           string               comment '所在街道' 
    ,address                        string               comment '详细地址' 
    ,post_code                      string               comment '邮政编码' 
    ,full_name                      string               comment '收货人姓名' 
    ,phone                          string               comment '收货人电话' 
    ,default_value                  int                  comment '是否默认0-非默认' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '收货地址' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_goods_product                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_product ( 
     product_id                     string               comment '产品id' 
    ,batch_id                       string               comment '批次id' 
    ,sku                            string               comment 'SKU' 
    ,goods_id                       string               comment '商品ID' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,product_weight                 int                  comment '一包的重量' 
    ,product_stock                  int                  comment '库存' 
    ,product_buy_min                int                  comment '期货卖出一包的重量' 
    ,product_buy_max                int                  comment '期货卖出库存' 
    ,product_unit                   string               comment '计量单位' 
    ,product_loading                int                  comment '是否下架(0审核1-上架2下架' 
    ,product_default                int                  comment '是否默认(1是默认)' 
    ,loading_at                     string               comment '上架时间' 
    ,unloading__at                  string               comment '下架时间' 
    ,product_sale_num               int                  comment '销售量' 
    ,product_location               int                  comment '排序字段' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,store_name                     string               comment '仓库名' 
) comment '商品产品' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_goods_goods                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_goods ( 
     goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,type_id                        string               comment '商品类型' 
    ,goods_img                      string               comment '商品主图' 
    ,goods_note                     int                  comment '0-农资商品' 
    ,goods_prop                     string               comment '商品等级' 
    ,goods_spec                     string               comment '规格详情' 
    ,goods_param                    string               comment '参数详情' 
    ,has_spec                       int                  comment '启用规格' 
    ,goods_unit                     string               comment '计量单位' 
    ,goods_loading                  int                  comment '是否下架(0-待上架，1-上架' 
    ,examine                        int                  comment '审核状态0-未审核' 
    ,goods_reject                   string               comment '驳回原因' 
    ,goods_view_num                 int                  comment '浏览量' 
    ,goods_comment_num              int                  comment '评论量' 
    ,goods_sale_num                 int                  comment '销售量' 
    ,goods_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记0-显示' 
    ,recom_sy                       int                  comment '0-优品优选' 
    ,recommend                      string               comment '商品推荐0-不推荐' 
    ,recom_xq                       int                  comment '邮费' 
    ,recom_gwc                      int                  comment '挂牌0-普通' 
    ,brand_id                       string               comment '商品品牌' 
    ,unloading_at                   string               comment '下架时间' 
    ,loading_at                     string               comment '上架时间' 
) comment '商品信息表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_goods_class                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_class ( 
     class_id                       bigint               comment '商品分类id' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_name                     string               comment '分类名称' 
    ,class_img                      string               comment '图片路径' 
    ,type_id                        string               comment '类型0-苗木分类' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,class_show                     tinyint              comment '是否显示(0-显示' 
    ,class_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '商品分类' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_goods_brand                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_brand ( 
     brand_id                       string               comment '品牌id' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,brand_img                      string               comment '图片地址' 
    ,brand_note                     string               comment '品牌介绍' 
    ,brand_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '商品品牌' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_member_icbc                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_icbc ( 
     icbc_id                        int                  comment '工行认证id' 
    ,member_id                      string               comment '会员id' 
    ,vendor_short_name              string               comment '企业用户简称' 
    ,vendor_name                    string               comment '企业用户名称，开通 E企付担保支付时必 输' 
    ,vendor_phone                   string               comment '企业用户联系电话(会员手机号' 
    ,vendor_email                   string               comment '企业用户邮箱' 
    ,province                       string               comment '所在省 code例130000' 
    ,city                           string               comment '所在市 code例130100' 
    ,county                         string               comment '所在区县code例130102' 
    ,address                        string               comment '详细地址' 
    ,postcode                       string               comment '邮政编码' 
    ,vendor_type                    string               comment '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户' 
    ,cert_type                      string               comment '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证' 
    ,cert_no                        string               comment '企业用户注册证件号码，' 
    ,cert_pic                       string               comment '企业用户注册证件图片' 
    ,cert_validityl_str             string               comment '企业用户注册证件有 效期' 
    ,operator_name                  string               comment '企业用户联系人姓名' 
    ,operator_mobile                string               comment '企业用户联系人手机' 
    ,operator_email                 string               comment '企业用户联系人邮箱' 
    ,operatorId_no                  string               comment '企业用户联系人身份 证号' 
    ,`status`                       int                  comment '状态0-未提交' 
    ,reason                         string               comment '失败原因' 
) comment '工行支付认证' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_township_area                               ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_township_area ( 
     id                             int                  comment 'id' 
    ,name_prov                      string               comment '省' 
    ,code_prov                      string               comment '省code' 
    ,name_city                      string               comment '市' 
    ,code_city                      string               comment '市code' 
    ,name_coun                      string               comment '区' 
    ,code_coun                      string               comment '区code' 
    ,name_town                      string               comment '镇' 
    ,code_town                      string               comment '镇code' 
) comment '省市区镇维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_shop_coupon                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_coupon ( 
     id                             string               comment 'ID' 
    ,shop_id                        string               comment '店铺id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,`type`                         int                  comment '优惠券类型(1-满减)' 
    ,sub_money                      double               comment '优惠金额' 
    ,enough_money                   double               comment '满减满足金额' 
    ,total_num                      int                  comment '总数量' 
    ,limit_sartAt                   string               comment '使用期限起' 
    ,limit_endAt                    string               comment '使用期限至' 
    ,limit_number                   int                  comment '允许单人领取数量' 
    ,has_score                      int                  comment '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      double               comment '所需余额' 
    ,`status`                       int                  comment '状态(0可用' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
) comment '全局优惠券维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_brand                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_brand ( 
     brand_id                       string               comment '品牌id' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,brand_img                      string               comment '图片地址' 
    ,brand_note                     string               comment '品牌介绍' 
    ,brand_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品品牌维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_class                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_class ( 
     class_id                       bigint               comment '商品分类id' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_name                     string               comment '分类名称' 
    ,class_img                      string               comment '图片路径' 
    ,type_id                        string               comment '类型0-苗木分类' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,class_show                     tinyint              comment '是否显示(0-显示' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品分类维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_member_address                              ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_member_address ( 
     address_id                     int                  comment '收货地址id' 
    ,member_id                      string               comment '会员ID' 
    ,province                       string               comment '所在省' 
    ,city                           string               comment '所在市' 
    ,area                           string               comment '所在县区' 
    ,town                           string               comment '所在街道' 
    ,address                        string               comment '详细地址' 
    ,post_code                      string               comment '邮政编码' 
    ,full_name                      string               comment '收货人姓名' 
    ,phone                          string               comment '收货人电话' 
    ,default_value                  int                  comment '是否默认0-非默认' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '收货地址维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_shop_shop                                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_shop ( 
     shop_id                        string               comment '店铺信息id' 
    ,shop_main                      string               comment '主营品种' 
    ,shop_user_id                   string               comment '商户ID' 
    ,`name`                         string               comment '商家昵称' 
    ,shop_name                      string               comment '店铺名称' 
    ,shop_logo                      string               comment '店铺图片' 
    ,shop_background                string               comment '背景图' 
    ,shop_desc                      string               comment '店铺简介' 
    ,shop_notice                    string               comment '店铺介绍内容' 
    ,shop_type                      int                  comment '认证类型0-个人，1-企业，2-经纪人' 
    ,shop_phone                     string               comment '店铺手机号' 
    ,shop_province                  string               comment '店铺所在省' 
    ,shop_city                      string               comment '店铺所在城市' 
    ,shop_area                      string               comment '店铺所在区域' 
    ,shop_town                      string               comment '乡镇' 
    ,shop_address                   string               comment '经营地址' 
    ,shop_star                      double               comment '信誉值(默认80)' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,create_id                      string               comment '操作人' 
    ,create_at                      string               comment '创建时间' 
    ,recommend                      int                  comment '首页展示' 
    ,shop_check                     int                  comment '审核状态' 
) comment '店铺维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_member_user                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_member_user ( 
     member_id                      string               comment '会员id' 
    ,member_phone                   string               comment '手机号' 
    ,member_name                    string               comment '昵称' 
    ,member_avator                  string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,`level`                        int                  comment '身份0-个人' 
    ,unionid                        string               comment '微信unionid' 
    ,openid                         string               comment '微信openId' 
    ,wx_name                        string               comment '微信昵称' 
    ,ali_name                       string               comment '阿里名字' 
    ,ali_account                    string               comment '阿里手机号' 
    ,icbc_id                        int                  comment '工行认证id' 
    ,vendor_short_name              string               comment '企业用户简称' 
    ,vendor_email                   string               comment '企业用户邮箱' 
    ,province                       string               comment '所在省 code例130000' 
    ,city                           string               comment '所在市 code例130100' 
    ,county                         string               comment '所在区县code例130102' 
    ,address                        string               comment '详细地址' 
    ,postcode                       string               comment '邮政编码' 
    ,vendor_type                    string               comment '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户' 
    ,cert_type                      string               comment '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证' 
    ,cert_no                        string               comment '企业用户注册证件号码，' 
    ,cert_pic                       string               comment '企业用户注册证件图片' 
    ,cert_validityl_str             string               comment '企业用户注册证件有 效期' 
    ,create_at                      string               comment '注册时间' 
    ,update_at                      string               comment '更新时间' 
) comment '会员维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_apply_purchase                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_apply_purchase ( 
     purchase_id                    int                  comment '采购id' 
    ,member_id                      string               comment '会员id' 
    ,purchase_num                   int                  comment '采购数量' 
    ,purchase_price                 double               comment '价格' 
    ,purchase_class                 string               comment '采购品种' 
    ,purchase_level                 string               comment '(弃用)' 
    ,address                        string               comment '收货地址' 
    ,fromword                       string               comment '期望货源地' 
    ,go                             string               comment '收获地' 
    ,create_at                      string               comment '创建时间' 
    ,create_by                      string               comment '创建者' 
    ,update_at                      string               comment '更新时间' 
    ,conent                         string               comment '内容(选填）' 
    ,image                          string               comment '图片地址' 
    ,del_flag                       string               comment '是否删除' 
    ,title                          string               comment '标题' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0发布中 1已结束 2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
) comment '采购信息' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_apply_supply                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_apply_supply ( 
     suppery_Id                     bigint               comment '供应id' 
    ,member_id                      string               comment '会员id' 
    ,goodclass                      string               comment '品类' 
    ,supper_price                   string               comment '供货价格' 
    ,`level`                        string               comment '商品等级' 
    ,fromword                       string               comment '货源地' 
    ,go                             string               comment '收获地' 
    ,image                          string               comment '图片' 
    ,conment                        string               comment '内容' 
    ,update_at                      string               comment '更新时间' 
    ,createby                       string               comment '创建者' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       string               comment '是否删除' 
    ,title                          string               comment '标题' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
    ,supperNum                      int                  comment '供货数量' 
) comment '供应信息' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_storage_lease                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_storage_lease ( 
     lease_id                       bigint               comment '用车id' 
    ,member_id                      string               comment '会员id' 
    ,title                          string               comment '标题' 
    ,price                          double               comment '价格' 
    ,local_no                       string               comment '冷库库位' 
    ,stcity                         string               comment '起运城市' 
    ,address                        string               comment '起运地详细地址' 
    ,encity                         string               comment '目的城市' 
    ,area                           string               comment '目的地地址' 
    ,content                        string               comment '备注' 
    ,image                          string               comment '图片' 
    ,create_at                      string               comment '发布时间' 
    ,create_by                      string               comment '创建人' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       string               comment '删除标记' 
) comment '我要用车' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_storage_rent                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_storage_rent ( 
     rent_id                        int                  comment '出租拉货id' 
    ,title                          string               comment '标题' 
    ,price                          double               comment '价格' 
    ,area                           string               comment '货车类型' 
    ,stcity                         string               comment '起运城市' 
    ,staddress                      string               comment '起运地址' 
    ,encity                         string               comment '目的城市' 
    ,address                        string               comment '目的地范围' 
    ,conent                         string               comment '备注内容' 
    ,image                          string               comment '图片' 
    ,create_at                      string               comment '发布时间' 
    ,create_by                      string               comment '创造者' 
    ,member_id                      string               comment '会员id' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览量' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,localNo                        string               comment '库位' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       string               comment '删除标记' 
) comment '我要拉货' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_ods_nstc.clife_nstc_ods_goods_type                                 ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_type ( 
     type_id                        string               comment '类型id' 
    ,shop_id                        string               comment '店铺id' 
    ,type_name                      string               comment '类型名称' 
    ,is_physical                    int                  comment '实物商品0-非实物' 
    ,has_spec                       int                  comment '使用规格0-不使用' 
    ,has_param                      int                  comment '使用参数0-不使用' 
    ,has_brand                      int                  comment '关联品牌0-不关联' 
    ,create_id                      string               comment '创建用户ID' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商品类型' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_type                                  ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_type ( 
     type_id                        string               comment '类型id' 
    ,shop_id                        string               comment '店铺id' 
    ,type_name                      string               comment '类型名称' 
    ,is_physical                    int                  comment '实物商品0-非实物' 
    ,has_spec                       int                  comment '使用规格0-不使用' 
    ,has_param                      int                  comment '使用参数0-不使用' 
    ,has_brand                      int                  comment '关联品牌0-不关联' 
    ,create_id                      string               comment '创建用户ID' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品类型维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_goods                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_goods ( 
     goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,shop_name                      string               comment '店铺名称' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,class_name                     string               comment '分类名称' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,type_id                        string               comment '商品类型' 
    ,type_name                      string               comment '类型名称' 
    ,goods_img                      string               comment '商品主图' 
    ,goods_note                     int                  comment '0-农资商品' 
    ,goods_prop                     string               comment '商品等级' 
    ,goods_spec                     string               comment '规格详情' 
    ,goods_param                    string               comment '参数详情' 
    ,has_spec                       int                  comment '启用规格' 
    ,goods_unit                     string               comment '计量单位' 
    ,goods_loading                  int                  comment '是否下架(0-待上架，1-上架' 
    ,recommend                      string               comment '商品推荐0-不推荐' 
    ,recom_xq                       int                  comment '邮费' 
    ,recom_gwc                      int                  comment '挂牌0-普通' 
    ,brand_id                       string               comment '商品品牌' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品信息表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dwd_order_info                                  ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dwd_order_info ( 
     order_id                       string               comment '订单id' 
    ,shop_id                        string               comment '店铺id' 
    ,shop_name                      string               comment '店铺名称' 
    ,member_id                      string               comment '会员ID' 
    ,member_name                    string               comment '会员名称' 
    ,good_ids                       string               comment '商品ids' 
    ,product_ids                    string               comment '产品ids' 
    ,order_sn                       string               comment '订单号' 
    ,merge_sn                       string               comment '合单号' 
    ,order_status                   int                  comment '订单状态' 
    ,order_price                    double               comment '订单总额' 
    ,order_create_time              string               comment '下单时间' 
    ,order_update_time              string               comment '订单更新时间' 
    ,coupon_id                      string               comment '优惠券id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,sub_money                      double               comment '优惠金额' 
    ,payment                        double               comment '实付金额' 
    ,pay_type                       int                  comment '支付类型' 
    ,pay_at                         string               comment '支付时间' 
    ,order_post                     double               comment '发货邮费' 
    ,shipping_type                  int                  comment '发货物流类型' 
    ,shipping_name                  string               comment '发货物流名称' 
    ,shipping_reminder              int                  comment '发货提醒' 
    ,consign_at                     string               comment '发货时间' 
    ,shipping_id                    string               comment '订单收货信息id' 
    ,receiver_name                  string               comment '收件人' 
    ,receiver_phone                 string               comment '电话' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,receiver_address               string               comment '地址' 
    ,receiving_at                   string               comment '收货时间' 
    ,coment                         int                  comment '是否评价' 
    ,appeal_id                      int                  comment '申诉id' 
    ,appeal_reason                  string               comment '申诉原因' 
    ,appeal_status                  int                  comment '申诉状态' 
    ,appeal_create_at               string               comment '申诉时间' 
    ,back_id                        string               comment '退换货id' 
    ,back_status                    int                  comment '退货审核状态' 
    ,back_price                     double               comment '退款金额' 
    ,back_type                      int                  comment '退货类型' 
    ,back_reason                    string               comment '退货原因' 
    ,return_at                      string               comment '退货时间' 
    ,back_way                       int                  comment '退货方式' 
    ,back_express_name              string               comment '退货物流名' 
    ,back_express_code              string               comment '退货快物流单号' 
    ,order_end_time                 string               comment '订单交易完成时间' 
    ,order_close_time               string               comment '订单交易关闭时间' 
) comment '订单多事物明细事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_product                               ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_product ( 
     product_id                     string               comment '产品id' 
    ,batch_id                       string               comment '批次id' 
    ,sku                            string               comment 'SKU' 
    ,goods_id                       string               comment '商品ID' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,product_weight                 int                  comment '一包的重量' 
    ,product_buy_min                int                  comment '期货卖出一包的重量' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,store_name                     string               comment '仓库名' 
    ,product_loading                int                  comment '是否下架' 
) comment '商品产品维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dwd_order_detail_info                           ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dwd_order_detail_info ( 
     detail_id                      string               comment '订单详情id' 
    ,order_id                       string               comment '订单ID' 
    ,product_id                     string               comment '产品规格ID' 
    ,member_id                      string               comment '买家ID' 
    ,goods_id                       string               comment '商品ID' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,class_name                     string               comment '商品分类名称' 
    ,type_id                        string               comment '商品类型' 
    ,type_name                      string               comment '商品类型名称' 
    ,brand_id                       string               comment '商品品牌' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,shop_id                        string               comment '店铺id' 
    ,shop_name                      string               comment '店铺名称' 
    ,recom_xq                       int                  comment '邮费' 
    ,sku                            string               comment 'SKU' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,total                          int                  comment '购买数量' 
    ,is_back                        int                  comment '是否退货：1 退货，0 未退货' 
    ,shipping_id                    string               comment '订单收货信息id' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,create_at                      string               comment '创建时间' 
) comment '产品交易明细事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dwd_order_detail_comment_info                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dwd_order_detail_comment_info ( 
     comment_id                     string               comment '评论表id' 
    ,product_id                     string               comment '产品id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '评论人' 
    ,star                           int                  comment '评价星级 1-5' 
    ,content                        string               comment '评价内容' 
    ,comment_img                    string               comment '评论图片' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,receiver_address               string               comment '地址' 
    ,create_at                      string               comment '创建时间' 
) comment '订单评价明细事实表' 
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


====================================================================================================
==========          db_dw_nstc.clife_nstc_dwd_deal_message_info                           ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dwd_deal_message_info ( 
     id                             bigint               comment '消息发布id' 
    ,title                          string               comment '标题' 
    ,member_id                      string               comment '会员id' 
    ,member_name                    string               comment '昵称' 
    ,goods_class                    string               comment '种类' 
    ,goods_num                      bigint               comment '买卖数量' 
    ,price                          double               comment '交易价格' 
    ,start_place                    string               comment '开始地' 
    ,end_place                      string               comment '目的地' 
    ,image                          string               comment '图片' 
    ,`comment`                      string               comment '备注内容' 
    ,`status`                       string               comment '发布状态' 
    ,scannum                        bigint               comment '浏览量' 
    ,action_type                    int                  comment '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
) comment '买卖信息发布明细事实表dwd' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


