create table if not exists db_dw_nstc.clife_nstc_dws_order_detail_comment_info ( 
     product_id                     string               comment '产品id' 
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '评论人id' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,comment_day                    string               comment '评论日期' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
) comment '用户评价明细汇聚表dws' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;