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