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