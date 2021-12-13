create table if not exists db_ods_nstc.clife_nstc_ods_member_address ( 
     address_id                     int                  comment '收货地址id' 
    ,member_id                      string               comment '会员ID' 
    ,province                       string               comment '所在省' 
    ,city                           string               comment '所在市' 
    ,area                           string               comment '所在县区' 
    ,town                           string               comment '所在街道' 
    ,address                        string               comment '详细地址' 
    ,post_code                      string               comment '邮政编码' 
    ,full_name                      string               comment '收货人姓名' 
    ,phone                          string               comment '收货人电话' 
    ,default_value                  int                  comment '是否默认0-非默认' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '收货地址' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;