create table if not exists db_ods_nstc.clife_nstc_ods_record_shop_member ( 
     id                             bigint               comment '店铺访问记录' 
    ,member_id                      string               comment '会员id' 
    ,shop_id                        string               comment '店铺id' 
    ,`type`                         int                  comment '新老访客0-新访客' 
    ,create_at                      string               comment '访问时间' 
    ,create_day                     string               comment '访问日期' 
) comment '店铺访问记录' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;