insert overwrite table db_ads_nstc.clife_nstc_ads_order_detail_comment_info_y partition(part_date)
select
     product_id                                                                           -- '产品id'
    ,substr(comment_day,0,4)                                as  comment_year              -- '评论日期（年）'
    ,sum(comment_cnt           )                            as  comment_cnt               -- '评价数'
    ,sum(zero_star_comment_cnt )                            as  zero_star_comment_cnt     -- '0星级评价数量'
    ,sum(one_star_comment_cnt  )                            as  one_star_comment_cnt      -- '1星级评价数量'
    ,sum(two_star_comment_cnt  )                            as  two_star_comment_cnt      -- '2星级评价数量'
    ,sum(three_star_comment_cnt)                            as  three_star_comment_cnt    -- '3星级评价数量'
    ,sum(four_star_comment_cnt )                            as  four_star_comment_cnt     -- '4星级评价数量'
    ,sum(five_star_comment_cnt )                            as  five_star_comment_cnt     -- '5星级评价数量'
    ,round(sum(zero_star_comment_cnt ) / sum(comment_cnt),2)  as  zero_star_comment_per     -- '0星级评价数量占比'
    ,round(sum(one_star_comment_cnt  ) / sum(comment_cnt),2)  as  one_star_comment_per      -- '1星级评价数量占比'
    ,round(sum(two_star_comment_cnt  ) / sum(comment_cnt),2)  as  two_star_comment_per      -- '2星级评价数量占比'
    ,round(sum(three_star_comment_cnt) / sum(comment_cnt),2)  as  three_star_comment_per    -- '3星级评价数量占比'
    ,round(sum(four_star_comment_cnt ) / sum(comment_cnt),2)  as  four_star_comment_per     -- '4星级评价数量占比'
    ,round(sum(five_star_comment_cnt ) / sum(comment_cnt),2)  as  five_star_comment_per     -- '5星级评价数量占比'
    ,max(part_date)              as  part_date                 -- '分区日期'
from db_dw_nstc.clife_nstc_dws_order_detail_comment_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 
group by product_id, substr(comment_day,0,4)
