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