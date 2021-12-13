create table if not exists db_ods_nstc.clife_nstc_ods_goods_type ( 
     type_id                        string               comment '类型id' 
    ,shop_id                        string               comment '店铺id' 
    ,type_name                      string               comment '类型名称' 
    ,is_physical                    int                  comment '实物商品0-非实物' 
    ,has_spec                       int                  comment '使用规格0-不使用' 
    ,has_param                      int                  comment '使用参数0-不使用' 
    ,has_brand                      int                  comment '关联品牌0-不关联' 
    ,create_id                      string               comment '创建用户ID' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商品类型' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;