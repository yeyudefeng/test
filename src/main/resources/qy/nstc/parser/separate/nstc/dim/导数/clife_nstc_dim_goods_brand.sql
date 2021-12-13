insert overwrite table db_dw_nstc.clife_nstc_dim_goods_brand partition(part_date)
select 
     brand_id                       -- '品牌id' 
    ,brand_name                     -- '品牌名称' 
    ,brand_url                      -- '品牌网址' 
    ,brand_img                      -- '图片地址' 
    ,brand_note                     -- '品牌介绍' 
    ,brand_location                 -- '排序字段' 
    ,create_id                      -- '创建用户id' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_brand
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 