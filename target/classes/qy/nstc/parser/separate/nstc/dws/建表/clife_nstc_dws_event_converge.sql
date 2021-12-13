create table if not exists db_dw_nstc.clife_nstc_dws_event_converge ( 
     member_id                      string               comment '会员id' 
    ,object_id                      string               comment '被操作对象id' 
    ,date_time                      string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,member_name                    string               comment '会员名称' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '用户行为汇聚表事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;