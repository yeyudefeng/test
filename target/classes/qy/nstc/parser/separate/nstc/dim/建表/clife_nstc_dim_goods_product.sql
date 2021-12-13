create table if not exists db_dw_nstc.clife_nstc_dim_goods_product ( 
     product_id                     string               comment '产品id' 
    ,batch_id                       string               comment '批次id' 
    ,sku                            string               comment 'SKU' 
    ,goods_id                       string               comment '商品ID' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,product_weight                 int                  comment '一包的重量' 
    ,product_buy_min                int                  comment '期货卖出一包的重量' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,store_name                     string               comment '仓库名' 
    ,product_loading                int                  comment '是否下架' 
) comment '商品产品维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;