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