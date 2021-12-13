create table if not exists db_ods_nstc.clife_nstc_ods_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`password`                     string               comment '密码' 
    ,token                          string               comment 'token' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,aurora_id                      string               comment '极光推送id' 
    ,disabled                       tinyint              comment '是否冻结账号(0-正常' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商家账号表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;