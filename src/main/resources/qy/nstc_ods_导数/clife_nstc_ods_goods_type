select 
     type_id                        -- 类型id
    ,shop_id                        -- 店铺id
    ,type_name                      -- 类型名称
    ,is_physical                    -- 实物商品0-非实物,1-实物
    ,has_spec                       -- 使用规格0-不使用,1-使用
    ,has_param                      -- 使用参数0-不使用,1-使用
    ,has_brand                      -- 关联品牌0-不关联,1-关联
    ,create_id                      -- 创建用户ID
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_type
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 