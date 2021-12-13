create table if not exists db_ods_nstc.clife_nstc_ods_member_shop ( 
     id                             bigint               comment '主键id' 
    ,shop_id                        string               comment '店铺id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '关注时间' 
) comment '店铺关注表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;