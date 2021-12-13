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