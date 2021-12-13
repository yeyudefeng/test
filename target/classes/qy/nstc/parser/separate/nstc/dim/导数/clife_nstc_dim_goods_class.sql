insert overwrite table db_dw_nstc.clife_nstc_dim_goods_class partition(part_date)
select  
     class_id                       -- '商品分类id' 
    ,parent_id                      -- '父级ID' 
    ,class_name                     -- '分类名称' 
    ,class_img                      -- '图片路径' 
    ,type_id                        -- '类型0-苗木分类,1-农资分类' 
    ,class_level                    -- '类型等级(0-大类,1-小类)' 
    ,class_show                     -- '是否显示(0-显示,1-隐藏)' 
    ,create_id                      -- '创建用户id' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_class
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 