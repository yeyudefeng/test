create table if not exists db_dw_nstc.clife_nstc_dim_shop_coupon ( 
     id                             string               comment 'ID' 
    ,shop_id                        string               comment '店铺id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,`type`                         int                  comment '优惠券类型(1-满减)' 
    ,sub_money                      double               comment '优惠金额' 
    ,enough_money                   double               comment '满减满足金额' 
    ,total_num                      int                  comment '总数量' 
    ,limit_sartAt                   string               comment '使用期限起' 
    ,limit_endAt                    string               comment '使用期限至' 
    ,limit_number                   int                  comment '允许单人领取数量' 
    ,has_score                      int                  comment '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      double               comment '所需余额' 
    ,`status`                       int                  comment '状态(0可用' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
) comment '全局优惠券维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;