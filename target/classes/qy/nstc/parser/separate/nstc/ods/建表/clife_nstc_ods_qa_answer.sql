create table if not exists db_ods_nstc.clife_nstc_ods_qa_answer ( 
     answer_id                      bigint               comment '答案id' 
    ,question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '回答人id' 
    ,answer_content                 string               comment '回答内容' 
    ,`status`                       int                  comment '显示状态0-显示' 
    ,like_num                       int                  comment '回复点赞数' 
    ,create_at                      string               comment '回答时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-答案表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;