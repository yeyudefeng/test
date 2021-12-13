select
     a.old_man_check_in_id     --老人入住id
    ,a.old_man_id              --老人id
    ,a.old_man_name            --姓名
    ,a.sex                     --性别
    ,if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr(a.birthday,5,7)))>=0,
        (substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)),
        (substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)-1)) as age --年龄
    ,a.agent_id                             --入住经办员工id
    ,j.create_user_ids                      --请假办理人id
    ,d.agent_id as countermand_agent_id    --退住经办人id
    ,p.membership_id                       --会籍id
    ,a.room_id                             --房间id
    ,a.membership_owner_id                 --权益人id
    ,a.contract_id                         --入住合同id
    ,k.relatives_names                     --探访人姓名
    ,a.nursin_homes_id                     --养老院id
    ,c.nursing_homes_name                  --养老院名称
    ,c.sub_org_id                          --机构id
    ,c.sub_org_name                        --机构名称
    ,c.org_id                              --集团id
    ,c.org_name                            --集团名称
    ,c.area_id                             --区域id
    ,a.reside_time                         --入住时间
    ,b.create_time as  contract_entry_time  --合同录入时间
    ,j.leave_create_times                  --请假时间（多值维度）
    ,k.visit_times                        --探访时间（多值维度）
    ,d.countermand_time                   --退住时间
    ,a.reside_status                      --入住状态
    ,a.estimated                          --是否评估（0-否 1-是）
    ,a.nursing_level_id                   --护理等级id
    ,a.nursing_level                      --护理等级名字
    ,a.deposited                          --是否缴纳定金
    ,a.charge_type                        --收费模式
    ,a.check_in_type                      --入住类型
    ,a.check_in_name                      --入住类型名称
    ,j.leave_times                        --请假时长（天为单位）
    ,j.accompany_names                    --陪同人姓名
    ,j.accompany_phones                   --陪同人电话
    ,j.leave_statuss                      --是否返回
    ,j.overtime_statuss                   --请假是否超时
    ,j.reality_times                      --实际请假天数
    ,j.leave_types                        --请假类型
    ,j.time_stages                        --外出0上午 1下午
    ,j.time_stage_returns                 --返回 0上午 1下午
    ,e.leave_day_cnt                      --当天请假次数
    ,f.refund_types                       --当天请假退费类型
    ,f.refund_amount                      --当天请假退费金额
    ,g.leave_reducation_fee               --当天请假减免费用
    ,h.visit_day_cnt                      --当天探访人数
    ,h.visit_time                         --当天探访时长
    ,20211024 as part_date
from db_ods_hr.clife_hr_ods_check_in a
left join db_ods_hr.clife_hr_ods_old_man_contract b on a.old_man_check_in_id = b.old_man_check_in_id and b.part_date=20211024
left join db_dw_hr.clife_hr_dim_nursing_home c on a.nursin_homes_id = c.nursing_homes_id and c.part_date=20211024
left join db_ods_hr.clife_hr_ods_old_man_countermand d on a.old_man_check_in_id = d.old_man_check_in_id and d.part_date=20211024
left join
(--请假表
  select
       t1.old_man_check_in_id
      ,t2.leave_day_cnt
      ,t2.leave_date
      ,t1.old_leave_id
  from
  (
    select
         rd.*
        ,row_number()over(partition by old_man_check_in_id order by create_time desc) as rank
    from db_ods_hr.clife_hr_ods_old_leave rd
    where part_date=20211024 and status = 1
  )t1
  left join
  (
    select
         old_man_check_in_id
        ,count(1) as leave_day_cnt --请假次数
        ,substr(create_time,1,10) as leave_date --请假日期
    from db_ods_hr.clife_hr_ods_old_leave
    where part_date=20211024 and status = 1
    group by old_man_check_in_id
            ,substr(create_time,1,10)
  )t2 on t1.old_man_check_in_id = t2.old_man_check_in_id and t2.leave_date=substr(t1.create_time,1,10)
  where t1.rank = 1
)e on a.old_man_check_in_id = e.old_man_check_in_id
left join
(--请假集合
  select
       old_man_check_in_id
      ,concat_ws(',',collect_list(cast(leave_time        as string))) as leave_times
      ,concat_ws(',',collect_list(cast(accompany_name    as string))) as accompany_names
      ,concat_ws(',',collect_list(cast(accompany_phone   as string))) as accompany_phones
      ,concat_ws(',',collect_list(cast(leave_status      as string))) as leave_statuss
      ,concat_ws(',',collect_list(cast(overtime_status   as string))) as overtime_statuss
      ,concat_ws(',',collect_list(cast(reality_time      as string))) as reality_times
      ,concat_ws(',',collect_list(cast(leave_type        as string))) as leave_types
      ,concat_ws(',',collect_list(cast(time_stage        as string))) as time_stages
      ,concat_ws(',',collect_list(cast(time_stage_return as string))) as time_stage_returns
      ,concat_ws(',',collect_list(cast(create_user_id as string))) as create_user_ids
      ,concat_ws(',',collect_list(cast(create_time as string))) as    leave_create_times
  from db_ods_hr.clife_hr_ods_old_leave  --请假
  where part_date=20211024 and status = 1
  group by old_man_check_in_id
)j on a.old_man_check_in_id=j.old_man_check_in_id
left join
(--请假退费表
  select
       old_man_check_in_id
      ,substr(create_time,1,10) as leave_refund_date --请假退费日期
      ,concat_ws(',',collect_list(cast(refund_type as string))) as refund_types
      ,sum(refund_amount) as refund_amount
  from db_ods_hr.clife_hr_ods_leave_refund  --bl请假退费表
  where part_date=20211024
    and state=1
  group by old_man_check_in_id
          ,substr(create_time,1,10)
)f on e.old_man_check_in_id = f.old_man_check_in_id and e.leave_date=f.leave_refund_date
left join
(--请假减免记录表
  select
       old_leave_id
      ,sum(leave_reducation_fee) as leave_reducation_fee
      ,substr(create_time,1,10) as leave_reduction_date
  from db_ods_hr.clife_hr_ods_leave_reduction   --请假减免记录表
  where part_date=20211024
    and status=1
  group by old_leave_id
          ,substr(create_time,1,10)
)g on e.old_leave_id=g.old_leave_id and e.leave_date=g.leave_reduction_date
left join
(--探访表
  select
       t4.old_man_id
      ,t5.visit_day_cnt
      ,t5.visit_date
      ,t5.visit_time
  from
  (
    select
         rd.*
        ,row_number()over(partition by old_man_id order by create_time desc) as rank
    from db_ods_hr.clife_hr_ods_relatives_visit rd
    where part_date=20211024 and state = 1
  )t4
  left join
  (
    select
         old_man_id
        ,count(1) as visit_day_cnt --每天探访人数
        ,substr(create_time,1,10) as visit_date --探访日期
        ,sum(CAST((unix_timestamp(visit_end_time) - unix_timestamp(visit_start_time)) / 60 AS int) % 60 ) as visit_time --当天探访时长
    from db_ods_hr.clife_hr_ods_relatives_visit
    where part_date=20211024 and state = 1
    group by old_man_id
            ,substr(create_time,1,10)
  )t5 on t4.old_man_id = t5.old_man_id and t5.visit_date=substr(t4.create_time,1,10)
  where t4.rank = 1
)h on a.old_man_id = h.old_man_id
left join
(--探访表集合
  select
       old_man_id
      ,concat_ws(',',collect_list(cast(relatives_name as string))) as relatives_names --探访人姓名
      ,concat_ws(',',collect_list(cast(visit_start_time as string))) as visit_times --探访时间
  from db_ods_hr.clife_hr_ods_relatives_visit
  where part_date=20211024 and state = 1
  group by old_man_id
)k on a.old_man_id = k.old_man_id
left join
(--会籍
  select
       a1.id as membership_id
      ,c1.old_man_id
  from db_ods_hr.clife_hr_ods_membership a1
  left join db_ods_hr.clife_hr_ods_account_account b1 on a1.user_id=b1.account_id
  left join db_ods_hr.clife_hr_ods_old_man_extend c1 on b1.phone=c1.linkman_phone
)p on a.old_man_id = p.old_man_id