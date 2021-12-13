create table if not exists db_dw_nstc.clife_nstc_dim_goods_brand ( 
     brand_id                       string               comment '品牌id' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,brand_img                      string               comment '图片地址' 
    ,brand_note                     string               comment '品牌介绍' 
    ,brand_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品品牌维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;