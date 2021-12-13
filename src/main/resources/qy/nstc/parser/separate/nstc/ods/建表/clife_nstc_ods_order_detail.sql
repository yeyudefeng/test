create table if not exists db_ods_nstc.clife_nstc_ods_order_detail ( 
     detail_id                      string               comment '订单详情id' 
    ,order_id                       string               comment '订单ID' 
    ,goods_id                       string               comment '商品ID' 
    ,product_id                     string               comment '产品规格ID' 
    ,order_sn                       string               comment '子订单号' 
    ,total                          int                  comment '购买数量' 
    ,coment                         int                  comment '0未评价  1已评价' 
    ,price                          double               comment '单价' 
    ,all_price                      double               comment '总价' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '订单详情表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;