create table if not exists db_dw_nstc.clife_nstc_dim_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,disabled                       int                  comment '是否冻结账号(0-正常' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商家账号维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;