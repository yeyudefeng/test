select 
     brand_id                       -- 品牌id
    ,brand_name                     -- 品牌名称
    ,brand_url                      -- 品牌网址
    ,brand_img                      -- 图片地址
    ,brand_note                     -- 品牌介绍
    ,brand_location                 -- 排序字段
    ,create_id                      -- 创建用户id
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_brand
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 
