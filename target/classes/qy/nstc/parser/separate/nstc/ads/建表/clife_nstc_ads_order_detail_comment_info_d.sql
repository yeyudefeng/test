create table if not exists db_ads_nstc.clife_nstc_ads_order_detail_comment_info_d ( 
     product_id                     string               comment '产品id' 
    ,comment_day                    string               comment '评论日期（日）' 
    ,comment_cnt                    bigint               comment '评价数' 
    ,zero_star_comment_cnt          bigint               comment '0星级评价数量' 
    ,one_star_comment_cnt           bigint               comment '1星级评价数量' 
    ,two_star_comment_cnt           bigint               comment '2星级评价数量' 
    ,three_star_comment_cnt         bigint               comment '3星级评价数量' 
    ,four_star_comment_cnt          bigint               comment '4星级评价数量' 
    ,five_star_comment_cnt          bigint               comment '5星级评价数量' 
    ,zero_star_comment_per          double               comment '0星级评价数量占比' 
    ,one_star_comment_per           double               comment '1星级评价数量占比' 
    ,two_star_comment_per           double               comment '2星级评价数量占比' 
    ,three_star_comment_per         double               comment '3星级评价数量占比' 
    ,four_star_comment_per          double               comment '4星级评价数量占比' 
    ,five_star_comment_per          double               comment '5星级评价数量占比' 
) comment '产品评价信息日应用表ads' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;