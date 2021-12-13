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


