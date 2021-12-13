insert overwrite table db_dw_nstc.clife_nstc_dim_goods_type partition(part_date)
select 
     type_id                        -- '类型id' 
    ,shop_id                        -- '店铺id' 
    ,type_name                      -- '类型名称' 
    ,is_physical                    -- '实物商品0-非实物,1-实物' 
    ,has_spec                       -- '使用规格0-不使用,1-使用' 
    ,has_param                      -- '使用参数0-不使用,1-使用' 
    ,has_brand                      -- '关联品牌0-不关联,1-关联' 
    ,create_id                      -- '创建用户ID' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_type
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0
