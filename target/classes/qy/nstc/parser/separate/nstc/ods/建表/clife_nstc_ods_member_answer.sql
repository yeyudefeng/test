create table if not exists db_ods_nstc.clife_nstc_ods_member_answer ( 
     member_id                      string               comment '会员id' 
    ,answer_id                      bigint               comment '回复id' 
) comment '回复点赞表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;