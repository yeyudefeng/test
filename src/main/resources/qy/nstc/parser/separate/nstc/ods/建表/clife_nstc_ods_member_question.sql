create table if not exists db_ods_nstc.clife_nstc_ods_member_question ( 
     member_id                      string               comment '会员id' 
    ,question_id                    bigint               comment '问题id' 
) comment '问题点赞表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;