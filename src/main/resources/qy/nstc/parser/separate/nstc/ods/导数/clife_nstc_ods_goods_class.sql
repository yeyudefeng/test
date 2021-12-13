select 
     class_id                       -- 商品分类id
    ,parent_id                      -- 父级ID
    ,class_name                     -- 分类名称
    ,class_img                      -- 图片路径
    ,type_id                        -- 类型0-苗木分类,1-农资分类
    ,class_level                    -- 类型等级(0-大类,1-小类)
    ,class_show                     -- 是否显示(0-显示,1-隐藏)
    ,class_location                 -- 排序字段
    ,create_id                      -- 创建用户id
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_class
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 