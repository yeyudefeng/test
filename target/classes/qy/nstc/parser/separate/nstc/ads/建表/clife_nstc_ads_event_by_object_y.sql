create table if not exists db_ads_nstc.clife_nstc_ads_event_by_object_y ( 
     object_id                      string               comment '被操作对象id' 
    ,year_date                      string               comment '业务日期' 
    ,object_type                    string               comment '对象类型' 
    ,object_name                    string               comment '被操作对象名称' 
    ,browse_num                     int                  comment '浏览量' 
    ,attention_num                  int                  comment '关注量' 
) comment '商品/店铺被浏览关注年应用表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;