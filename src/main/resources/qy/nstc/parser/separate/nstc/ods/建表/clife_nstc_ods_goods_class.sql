create table if not exists db_ods_nstc.clife_nstc_ods_goods_class ( 
     class_id                       bigint               comment '商品分类id' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_name                     string               comment '分类名称' 
    ,class_img                      string               comment '图片路径' 
    ,type_id                        string               comment '类型0-苗木分类' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,class_show                     tinyint              comment '是否显示(0-显示' 
    ,class_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '商品分类' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;