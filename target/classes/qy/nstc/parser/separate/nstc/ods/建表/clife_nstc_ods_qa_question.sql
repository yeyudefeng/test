create table if not exists db_ods_nstc.clife_nstc_ods_qa_question ( 
     question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '会员id' 
    ,question_content               string               comment '问题描述' 
    ,question_img                   string               comment '图片链接' 
    ,`status`                       int                  comment '显示状态0-显示' 
    ,like_num                       int                  comment '点赞数' 
    ,create_at                      string               comment '发布时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-问题表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;