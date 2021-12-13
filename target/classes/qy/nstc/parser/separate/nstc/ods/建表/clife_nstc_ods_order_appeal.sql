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