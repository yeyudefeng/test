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