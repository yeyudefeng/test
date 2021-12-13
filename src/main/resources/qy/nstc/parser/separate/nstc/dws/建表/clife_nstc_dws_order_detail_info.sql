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