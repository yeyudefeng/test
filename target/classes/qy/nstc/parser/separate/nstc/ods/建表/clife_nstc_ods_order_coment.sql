create table if not exists db_ods_nstc.clife_nstc_ods_order_coment ( 
     comment_id                     string               comment '评论表id' 
    ,detail_id                      string               comment '订单详情表id' 
    ,goods_id                       string               comment '商品id' 
    ,product_id                     string               comment '产品id' 
    ,member_id                      string               comment '评论人' 
    ,star                           int                  comment '评价星级 1-5' 
    ,content                        string               comment '评价内容' 
    ,comment_img                    string               comment '评论图片' 
    ,disply                         int                  comment '删除标记 0：未删除，1：删除' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '评价表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;