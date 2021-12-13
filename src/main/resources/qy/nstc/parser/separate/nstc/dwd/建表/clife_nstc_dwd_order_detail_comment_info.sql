create table if not exists db_dw_nstc.clife_nstc_dwd_order_detail_comment_info ( 
     comment_id                     string               comment '评论表id' 
    ,product_id                     string               comment '产品id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '评论人' 
    ,star                           int                  comment '评价星级 1-5' 
    ,content                        string               comment '评价内容' 
    ,comment_img                    string               comment '评论图片' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,receiver_address               string               comment '地址' 
    ,create_at                      string               comment '创建时间' 
) comment '订单评价明细事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;