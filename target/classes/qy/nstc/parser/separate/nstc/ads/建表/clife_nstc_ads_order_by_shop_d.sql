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