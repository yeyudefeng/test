create table if not exists db_dw_nstc.clife_nstc_dwd_event_info ( 
     id                             bigint               comment '记录id' 
    ,member_id                      string               comment '会员id' 
    ,member_name                    string               comment '会员名称' 
    ,object_id                      string               comment '被操作对象id （商品 店铺）' 
    ,object_name                    string               comment '被操作对象名称' 
    ,object_type                    string               comment '对象类型' 
    ,behavior_type                  int                  comment '行为动作类型' 
    ,`type`                         int                  comment '是否是新客户' 
    ,create_at                      string               comment '行为时间' 
) comment '用户行为明细事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;