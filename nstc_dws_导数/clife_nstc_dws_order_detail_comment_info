insert overwrite table db_dw_nstc.clife_nstc_dws_order_detail_comment_info partition(part_date)
select 
 product_id                                                              -- '产品id'
,goods_id                                                                -- '商品id'
,member_id                                                               -- '评论人id'
,receiver_province                                                       -- '省份'
,receiver_city                                                           -- '城市'
,receiver_area                                                           -- '区县'
,receiver_town                                                           -- '街道'
,substr(create_at,0,10)                      as comment_day              -- '评论日期'
,count(1)                                    as comment_cnt              -- '评价数'
,sum(case when star = 0 then 1 else 0 end)   as zero_star_comment_cnt    -- '0星级评价数量'
,sum(case when star = 1 then 1 else 0 end)   as one_star_comment_cnt     -- '1星级评价数量'
,sum(case when star = 2 then 1 else 0 end)   as two_star_comment_cnt     -- '2星级评价数量'
,sum(case when star = 3 then 1 else 0 end)   as three_star_comment_cnt   -- '3星级评价数量'
,sum(case when star = 4 then 1 else 0 end)   as four_star_comment_cnt    -- '4星级评价数量'
,sum(case when star = 5 then 1 else 0 end)   as five_star_comment_cnt    -- '5星级评价数量'
,max(part_date)                              as part_date                     -- '分区日期'
from db_dw_nstc.clife_nstc_dwd_order_detail_comment_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, goods_id, member_id, receiver_province, receiver_city, receiver_area, receiver_town, substr(create_at,0,10)