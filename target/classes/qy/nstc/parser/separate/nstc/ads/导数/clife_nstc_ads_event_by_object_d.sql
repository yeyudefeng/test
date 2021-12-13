insert overwrite table db_ads_nstc.clife_nstc_ads_event_by_object_d partition(part_date)
select 
     object_id     --被操作对象id
    ,date_time     --业务日期
    ,max(object_type)        as  object_type  --对象类型
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(browse_num)    as browse_num    --浏览量
    ,sum(attention_num) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dws_event_converge
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  object_id
         ,date_time