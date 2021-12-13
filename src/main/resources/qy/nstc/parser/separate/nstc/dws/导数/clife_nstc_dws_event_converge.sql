insert overwrite table db_dw_nstc.clife_nstc_dws_event_converge partition(part_date)
select 
     member_id                                --会员id
    ,object_id                                --被操作对象id
    ,substr(create_at,1,10)  as date_time     --业务日期
    ,max(object_type)        as  object_type  --对象类型
    ,max(member_name)        as  member_name  --会员名称
    ,max(object_name)        as  object_name  --被操作对象名称
    ,sum(case when behavior_type=0 then 1 else 0 end) as browse_num    --浏览量
    ,sum(case when behavior_type=1 then 1 else 0 end) as attention_num --关注量
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_nstc.clife_nstc_dwd_event_info
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by  member_id
         ,object_id
         ,substr(create_at,1,10)