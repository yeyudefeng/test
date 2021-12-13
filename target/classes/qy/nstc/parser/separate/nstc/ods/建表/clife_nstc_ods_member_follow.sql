create table if not exists db_ods_nstc.clife_nstc_ods_member_follow ( 
     follow_id                      bigint               comment '主键id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '创建时间' 
) comment '商品关注表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;