================================================================================
==========          clife_hr_dwd_reside_manage_info                   ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_reside_manage_info partition(part_date)
select
     a.old_man_check_in_id     --老人入住id
    ,a.old_man_id              --老人id
    ,a.old_man_name            --姓名
    ,a.sex                     --性别
    ,if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr(a.birthday,5,7)))>=0,
(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)),
(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)-1)) as age --年龄
    ,a.agent_id                            --入住经办员工id
    ,j.create_user_ids                     --请假办理人id
    ,d.agent_id as countermand_agent_id    --退住经办人id
    ,p.id as membership_id                 --会籍id
    ,a.room_id                             --房间id
    ,a.bed_id                              --床位id
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
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_check_in a
left join db_ods_hr.clife_hr_ods_old_man_contract b on a.old_man_check_in_id = b.old_man_check_in_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_nursing_home c on a.nursin_homes_id = c.nursing_homes_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_old_man_countermand d on a.old_man_check_in_id = d.old_man_check_in_id and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(--请假表
  select  
       old_man_check_in_id
      ,count(1) as leave_day_cnt --当天请假次数
  from db_ods_hr.clife_hr_ods_old_leave
  where part_date=regexp_replace(date_sub(current_date(),1),'-','') 
  and status = 1
  and substr(start_time,1,10) = regexp_replace(date_sub(current_date(),1),'-','')
  group by old_man_check_in_id
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
  where part_date=regexp_replace(date_sub(current_date(),1),'-','') and status = 1
  group by old_man_check_in_id
)j on a.old_man_check_in_id=j.old_man_check_in_id
left join 
(--请假退费表
  select 
       old_man_check_in_id
      ,concat_ws(',',collect_list(cast(refund_type as string))) as refund_types  --当天请假退费类型
      ,sum(refund_amount) as refund_amount  --当天请假退费金额
  from db_ods_hr.clife_hr_ods_leave_refund  --bl请假退费表
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and state=1
    and substr(create_time,1,10)=regexp_replace(date_sub(current_date(),1),'-','')
  group by old_man_check_in_id
)f on a.old_man_check_in_id = f.old_man_check_in_id
left join 
(--请假减免记录表
   select
        a.old_man_check_in_id
       ,sum(b.leave_reducation_fee) as leave_reducation_fee  --当天请假减免费用
   from db_ods_hr.clife_hr_ods_old_leave a
   left join
   (
     select
          old_leave_id
         ,sum(leave_reducation_fee) as leave_reducation_fee  --当天请假减免费用
     from db_ods_hr.clife_hr_ods_leave_reduction   --请假减免记录表
     where part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and status=1
       and substr(create_time,1,10)=regexp_replace(date_sub(current_date(),1),'-','')
     group by old_leave_id
   )b on a.old_leave_id = b.old_leave_id
   where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by  a.old_man_check_in_id
)g on a.old_man_check_in_id = g.old_man_check_in_id
left join
(--探访表
    select  
         old_man_id
        ,count(1) as visit_day_cnt --每天探访人数
        ,substr(visit_start_time,1,10) as visit_date --探访日期
        ,sum(CAST((if(visit_end_time is null,unix_timestamp(),unix_timestamp(visit_end_time)) - unix_timestamp(visit_start_time)) / 60 AS int) % 60) as visit_time --当天探访时长
    from db_ods_hr.clife_hr_ods_relatives_visit
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
	 and state = 1
     and substr(visit_start_time,1,10)=regexp_replace(date_sub(current_date(),1),'-','')
    group by old_man_id 
            ,substr(visit_start_time,1,10) 
)h on a.old_man_id = h.old_man_id
left join 
(--探访表集合
  select  
       old_man_id
      ,concat_ws(';',collect_list(cast(relatives_name as string)))   as relatives_names --探访人姓名
      ,concat_ws(',',collect_list(cast(concat(visit_start_time,'-',visit_end_time) as string))) as visit_times --探访时间
  from db_ods_hr.clife_hr_ods_relatives_visit
  where part_date=regexp_replace(date_sub(current_date(),1),'-','') and state = 1
  group by old_man_id
)k on a.old_man_id = k.old_man_id
left join db_ods_hr.clife_hr_ods_membership p on a.relation_id = p.id and p.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dwd_activity_manage_info                 ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_activity_manage_info partition(part_date)
select
     a.activity_id          --'活动id'
    ,a.create_user_id       --'经办人id'
    ,b.reserve_ids          --'预定活动室id'
    ,a.activity_room_id     --活动室id
    ,a.template_id          --活动模板id
    ,a.activity_type        --活动类型
    ,a.nurse_home_id        --养老院id
    ,j.nursing_homes_name   --养老院名称
    ,a.sub_org_id           --机构id
    ,j.sub_org_name         --机构名称
    ,a.org_id               --集团id
    ,j.org_name             --集团名称
    ,j.area_id              --区域id
    ,a.create_time as activity_create_time   --活动创建时间
    ,b.reserve_times         --预定活动室时间
    ,b.reserve_user_ids      --预约人
    ,b.reserve_users         --预约人姓名
    ,b.reserve_uses          --预定用途
    ,b.reserve_user_types    --预定人类型
    ,a.activity_time         --活动举办时间
    ,a.apply_start_time      --活动开始报名时间
    ,a.apply_time            --活动结束报名时间
    ,a.activity_start        --活动开始时间
    ,a.activity_end          --活动结束时间
    ,e.old_man_ids           --邀请老人（多值）
    ,e.old_man_cnt           --邀请老人数
    ,f.apply_old_mans         --报名老人
    ,f.apply_old_man_cnt  --报名老人数
    ,f.sign_old_mans      --签到老人
    ,f.sign_old_man_cnt   --签到人数
    ,f.confirm_old_mans   --到场老人
    ,f.confirm_old_man_cnt --到场人数
    ,a.activity_fee        --活动费用
    ,g.good_evaluation_num    --活动评价好评数
    ,g.middle_evaluation_num  --活动评价中评数
    ,g.bad_evaluation_num     --活动评价差评数
    ,h.good_feedback_num    --反馈非常好数
    ,h.middle_feedback_num  --反馈较好数
    ,h.general_feedback_num --反馈一般数
    ,h.bad_feedback_num     --反馈不理想数
    ,f.sign_times           --签到时间
    ,f.confirm_times        --到场时间
    ,e.apply_times          --报名时间
    ,g.evaluation_times --评价时间
    ,h.feedback_times --反馈时间
    ,g.evaluation_types --评价状态
	,h.feedback_types --反馈的状态
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from db_ods_hr.clife_hr_ods_org_activity a
left join 
(--预定活动室
   select
        activity_id
       ,concat_ws(';',collect_list(cast(reserve_time      as string))) as reserve_times     
       ,concat_ws(';',collect_list(cast(reserve_user_id    as string)))as reserve_user_ids  
       ,concat_ws(';',collect_list(cast(reserve_user   as string))) as reserve_users     
       ,concat_ws(';',collect_list(cast(reserve_use      as string))) as reserve_uses      
       ,concat_ws(';',collect_list(cast(reserve_user_type   as string))) as reserve_user_types
       ,concat_ws(';',collect_list(cast(id   as string))) as reserve_ids
   from db_ods_hr.clife_hr_ods_org_activity_room_reserve 
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
     and type=1
   group by activity_id
)b on a.activity_id = b.activity_id
left join 
(--邀请表
  select 
       activity_id
      ,concat_ws(';',collect_list(cast(case when apply_type = 1 then old_man_id else null end as string))) as old_man_ids
      ,count(distinct old_man_id) as old_man_cnt
      ,concat_ws(';',collect_list(cast(case when apply_type = 2 then create_time else null end as string)))  as apply_times
  from db_ods_hr.clife_hr_ods_org_activity_apply
  where part_date=regexp_replace(date_sub(current_date(),1),'-','') 
  group by activity_id
)e on a.activity_id = e.activity_id
left join 
(--参与人表
  select 
       activity_id
      ,sum(case when apply_flag = 1 then attach_num else 0 end) as apply_old_man_cnt
      ,sum(case when sign_status = 2 then 1 else 0 end) as sign_old_man_cnt
      ,sum(case when sign_time is not null then 1 else 0 end) as confirm_old_man_cnt
      ,concat_ws(';',collect_list(cast(case when apply_flag = 1 then old_man_id else null end as string)))  as apply_old_mans
      ,concat_ws(';',collect_list(cast(case when sign_status = 2 then old_man_id else null end as string))) as sign_old_mans
      ,concat_ws(';',collect_list(cast(case when sign_status = 2 then sign_time else null end as string)))  as sign_times
      ,concat_ws(';',collect_list(cast(old_man_id as string))) as confirm_old_mans
      ,concat_ws(';',collect_list(cast(join_time as string)))  as confirm_times --参加时间
  from db_ods_hr.clife_hr_ods_org_activity_participant
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and del_flag=1
  group by activity_id
)f on a.activity_id = f.activity_id
left join 
(--评价表
  select
       activity_id
      ,sum(case when evaluation_type=1 then 1 else 0 end ) as good_evaluation_num    --活动评价好评数
      ,sum(case when evaluation_type=2 then 1 else 0 end ) as middle_evaluation_num  --活动评价中评数
      ,sum(case when evaluation_type=3 then 1 else 0 end ) as bad_evaluation_num     --活动评价差评数
      ,concat_ws(';',collect_list(cast(evaluation_type as string)))  as evaluation_types --评价状态
      ,concat_ws(';',collect_list(cast(create_time as string)))  as evaluation_times --评价时间
  from db_ods_hr.clife_hr_ods_org_activity_evaluation
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by activity_id
)g on a.activity_id = g.activity_id
left join 
(
  select
       activity_id
      ,sum(case when feedback_type=1 then 1 else 0 end ) as good_feedback_num    --反馈非常好数
      ,sum(case when feedback_type=2 then 1 else 0 end ) as middle_feedback_num  --反馈较好数
      ,sum(case when feedback_type=3 then 1 else 0 end ) as general_feedback_num --反馈一般数
      ,sum(case when feedback_type=4 then 1 else 0 end ) as bad_feedback_num     --反馈不理想数
      ,concat_ws(';',collect_list(cast(feedback_type as string))) as feedback_types --反馈的状态
      ,concat_ws(';',collect_list(cast(create_time as string)))  as feedback_times --反馈时间
  from db_ods_hr.clife_hr_ods_org_activity_feedback
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by activity_id
)h on a.activity_id = h.activity_id
left join db_dw_hr.clife_hr_dim_nursing_home j on a.nurse_home_id = j.nursing_homes_id and j.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dwd_coupon_manage_info                   ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_coupon_manage_info partition(part_date)
select
     a.id as coupon_id     --优惠券id
    ,a.coupon_name         --优惠券名称
    ,a.type                --优惠券类型
    ,a.attributes          --公寓属性
    ,a.apartment_name      --公寓名称
    ,a.coupon_amount       --优惠券额度
    ,a.period                     --期数
    ,a.preferential_plan        --优惠方案
    ,a.status                   --优惠券状态
    ,a.valid_period_start    --有效期开始时间
    ,a.valid_period_end      --有效期结束时间
    ,c.create_user_id        --经办人id
    ,a.user_id               --领取人id
    ,a.use_name              --使用人姓名
    ,a.nurse_home_id         --养老院id
    ,d.nursing_homes_name    --养老院名称
    ,a.sub_org_id            --机构id
    ,a.sub_org_name          --机构名称
    ,a.org_id                --集团id
    ,a.org_name              --集团名称
    ,d.area_id               --区域id
    ,b.coupon_issue_times    --发放时间
    ,a.use_date              --使用时间
    ,a.use_frequency        --使用次数
    ,a.balance              --余额
    ,a.last_balance         --上次余额
    ,a.balance - a.last_balance as cost  --花费
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from 
(
  select
       t1.*
      ,t4.nurse_home_id --养老院id
      ,t2.sub_org_name  --机构名称
      ,t2.org_name      --集团名称
      ,lag(t1.balance, 1, null) over(partition by t1.id order by t1.use_date) as last_balance --上次余额
  from db_ods_hr.clife_hr_ods_coupon t1
  left join db_dw_hr.clife_hr_dim_admin_sub_org t2 on t1.sub_org_id = t2.sub_org_id  and t2.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  left join db_ods_hr.clife_hr_ods_org_nurse t4 on t1.sub_org_id=t4.sub_org_id and t1.org_id=t4.org_id and t4.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  where t1.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and t1.status = 1
)a
left join db_ods_hr.clife_hr_ods_coupon_setting c on a.coupon_setting_id = c.id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
  select 
       coupon_setting_id
      ,concat_ws(';',collect_list(cast(create_time as string))) as coupon_issue_times
  from db_ods_hr.clife_hr_ods_coupon_issue
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and status=1
  group by coupon_setting_id
)b on a.coupon_setting_id = b.coupon_setting_id
left join db_ods_hr.clife_hr_ods_nursing_homes_baseinfo d on a.nurse_home_id = d.nursing_homes_id


================================================================================
==========          clife_hr_dwd_plan_snapshot_info                   ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_plan_snapshot_info partition(part_date)
select
a.snap_item_id        --计划ID
,a.user_id             --老人id
,c.nursing_homes_id    --养老院id
,c.area_id             --养老院区域id
,a.sub_org_id          --机构id
,c.sub_org_name        --机构名称
,c.org_id              --集团id
,c.org_name            --集团名称
,a.project_id          --项目id
,a.project_name        --项目名称
,a.project_type_id     --项目类型id
,a.project_type_name   --项目类型名称
,a.frequency_type      --频次类型
,a.frequency_num       --频次数
,a.count               --完成计数
,a.exec_date           --执行日
,a.complete_stase      --完成状态
,b.create_time as plan_create_time  --计划创建时间
,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from db_ods_hr.clife_hr_ods_nurse_plan_snapshot a
left join db_ods_hr.clife_hr_ods_basic_service_plan b on a.plan_id=b.plan_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_nursing_home c on a.sub_org_id=c.sub_org_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dwd_recharge_consumption_info            ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_recharge_consumption_info partition(part_date)
select
     p.id                        --记录id
    ,p.old_man_id                --老人id
    ,p.member_id                 --会员id
    ,n.old_man_name            --姓名
    ,n.sex                     --性别
    ,n.age                     --年龄
    ,m.nursing_homes_id        --养老机构id
    ,m.nursing_homes_name      --养老院名称
    ,p.sub_org_id              --机构id
    ,m.sub_org_name            --机构名称
    ,p.org_id                  --集团id
    ,m.org_name                --集团名称
    ,m.area_id                 --区域id
    ,p.account_id                --账户id
    ,p.remark                    --备注
    ,p.amount                   --账户充值的金额
    ,p.recharge_consumption_time --充值时间   
    ,p.behavior_type             --行为类型
    ,p.consumption_type          --消费类型
    ,p.fee_type_id               --费用类型ID
    ,p.fee_type_name             --费用类型名称
    ,p.fee_iteam_id              --费用项目id
    ,p.fee_iteam_name            --费用项目名称
    ,p.bill_child_id             --账单子表ID
    ,p.bill_id                   --账单id
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
--充值
select
     a.id                 --记录id
    ,null as old_man_id --老人id
    ,a.member_id          --会员id
    ,a.sub_org_id         --机构id
    ,a.org_id             --集团id
    ,b.id as account_id   --账户id
    ,a.remark             --备注
    ,a.recharge_amount as amount   --账户充值的金额
    ,a.pay_time       as recharge_consumption_time    --充值时间   
    ,'会员充值' as behavior_type  --行为类型
    ,1 as consumption_type        --消费类型
    ,null as fee_type_id    --费用类型ID(充值null,账务退住为null)
    ,null as fee_type_name  --费用类型名称(充值null,账务退住为null)
    ,null as fee_iteam_id   --费用项目id(充值null,账务退住为null)
    ,null as fee_iteam_name --费用项目名称(充值null,账务退住为null)
    ,null as bill_child_id  --账单子表ID(充值null,账务退住为null)
    ,null as bill_id        --账单id(充值null,账务退住为null) 
from db_ods_hr.clife_hr_ods_member_recharge_record a
left join db_ods_hr.clife_hr_ods_member_account b on a.member_id = b.member_id and b.id<>b.member_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and a.state = 1
union all
select
     id              --记录id
    ,old_man_id      --老人id
    ,null as member_id --会员id
    ,sub_org_id      --机构id
    ,org_id          --集团id
    ,account_id      --账户id
    ,remark          --备注
    ,fee_amount  as amount    --账户充值的金额
    ,pay_time      as recharge_consumption_time  --充值时间
    ,'账户充值' as behavior_type  --行为类型
    ,1 as consumption_type        --消费类型
    ,null as fee_type_id          --费用类型ID(充值null,账务退住为null)
    ,null as fee_type_name        --费用类型名称(充值null,账务退住为null)
    ,null as fee_iteam_id   --费用项目id(充值null,账务退住为null)
    ,null as fee_iteam_name --费用项目名称(充值null,账务退住为null)
    ,null as bill_child_id  --账单子表ID(充值null,账务退住为null)
    ,null as bill_id        --账单id(充值null,账务退住为null)
from db_ods_hr.clife_hr_ods_recharge_record
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and state = 1
union all
--消费
select 
     a.fee_detail_record_id as id  --消费流水id
    ,a.old_man_id           --老人ID
    ,null as member_id      --会员id
    ,a.sub_org_id           --机构id
    ,a.org_id               --集团id
    ,e.account_id           --账户id
    ,a.remark               --备注
    ,a.fee_amount        as amount   --金额
    ,a.consumption_time   as recharge_consumption_time  --消费时间
    ,'账户消费' as behavior_type  --行为类型
    ,0 as consumption_type        --消费类型
    ,a.fee_type_id          --费用类型ID(充值null,账务退住为null)
    ,a.fee_type_name        --费用类型名称(充值null,账务退住为null)
    ,a.fee_iteam_id         --费用项目id(充值null,账务退住为null)
    ,a.fee_iteam_name       --费用项目名称(充值null,账务退住为null)
    ,a.child_id as bill_child_id --账单子表ID(充值null,账务退住为null)
    ,c.id as bill_id             --账单ID(充值null,账务退住为null)
from db_ods_hr.clife_hr_ods_detail_record a
left join db_ods_hr.clife_hr_ods_month_bill_child b on a.child_id = b.id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_month_bill c on b.month_bill_id = c.id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
  select
       old_man_check_in_id
      ,old_man_id
      ,room_attribute
      ,case when check_in_type=0 then 1
            when check_in_type=1 then 1
            when check_in_type=2 then 2
            when check_in_type=3 then 2
            when check_in_type=4 then 2
       else check_in_type end as check_in_type
  from db_ods_hr.clife_hr_ods_check_in
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and status =1
)d on c.old_man_check_in_id = d.old_man_check_in_id
left join db_ods_hr.clife_hr_ods_account e on d.old_man_id=e.old_man_id and e.symbol=1 and e.attributes=d.check_in_type and e.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and a.state = 1
union all
select
     a.fee_detail_record_id as id --记录id
    ,null as old_man_id    --老人id
    ,a.member_id             --会员id
    ,a.sub_org_id            --机构ID
    ,a.org_id                --集团ID
    ,b.id as account_id   --账户id
    ,null as remark   --备注
    ,a.fee_amount as amount--金额
    ,a.create_time as recharge_consumption_time  --消费时间
    ,'会员消费' as behavior_type  --行为类型
    ,0 as consumption_type        --消费类型
    ,a.fee_type_id          --费用类型ID(充值null,账务退住为null)
    ,a.fee_type_name        --费用类型名称(充值null,账务退住为null)
    ,a.fee_iteam_id         --费用项目id(充值null,账务退住为null)
    ,a.fee_iteam_name       --费用项目名称(充值null,账务退住为null)
    ,null as bill_child_id  --账单子表ID(充值null,账务退住为null)
    ,null as bill_id        --账单ID(充值null,账务退住为null)
from db_ods_hr.clife_hr_ods_member_detail_record a
left join db_ods_hr.clife_hr_ods_member_account b on a.member_id = b.member_id and b.id<>b.member_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and a.state = 1
union all
--入住缴费
select
     a.id  --消费流水id
    ,a.old_man_id           --老人ID
    ,null as member_id      --会员id
    ,a.sub_org_id           --机构id
    ,a.org_id               --集团id
    ,e.account_id           --账户id
    ,null as remark         --备注
    ,a.fee_amount   as amount     --金额
    ,a.pay_time     as recharge_consumption_time  --入住缴费时间
    ,'入住缴费'     as behavior_type  --行为类型
    ,0 as consumption_type        --消费类型
    ,f.fee_type_id          --费用类型ID(充值null,账务退住为null)
    ,g.fee_type as fee_type_name        --费用类型名称(充值null,账务退住为null)
    ,a.fee_item_id as  fee_iteam_id       --费用项目id(充值null,账务退住为null)
    ,a.fee_text as fee_iteam_name --费用项目名称(充值null,账务退住为null)
    ,null as bill_child_id --账单子表ID(充值null,账务退住为null)
    ,null as bill_id             --账单ID(充值null,账务退住为null)
from db_ods_hr.clife_hr_ods_checkin_pay_record a 
left join db_ods_hr.clife_hr_ods_account e on a.old_man_id=e.old_man_id and e.symbol=1 and e.attributes=a.reside_type and e.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_fee_item f on a.fee_item_id=f.fee_item_id and f.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_fee_type g on f.fee_type_id=g.fee_type_id and g.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.state=1 and a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
union all
--退住缴费
select
     a.hr_inform_id as id  --消费流水id
    ,a.old_man_id           --老人ID
    ,null as member_id      --会员id
    ,a.sub_org_id           --机构id
    ,a.org_id               --集团id
    ,e.account_id           --账户id
    ,a.handle_content as remark  --备注
    ,a.sum_cost_money       as amount    --金额
    ,a.handle_time   as recharge_consumption_time  --消费时间
    ,'退住缴费' as behavior_type  --行为类型
    ,0 as consumption_type        --消费类型
    ,null as fee_type_id          --费用类型ID(充值null,账务退住为null)
    ,null as fee_type_name        --费用类型名称(充值null,账务退住为null)
    ,null as fee_iteam_id         --费用项目id(充值null,账务退住为null)
    ,null as fee_iteam_name       --费用项目名称(充值null,账务退住为null)
    ,null as bill_child_id        --账单子表ID(充值null,账务退住为null)
    ,null as bill_id              --账单ID(充值null,账务退住为null)
from db_ods_hr.clife_hr_ods_inform a
left join 
(
  select
       old_man_check_in_id
      ,old_man_id
      ,room_attribute
      ,case when check_in_type=0 then 1
            when check_in_type=1 then 1
            when check_in_type=2 then 2
            when check_in_type=3 then 2
            when check_in_type=4 then 2
       else check_in_type end as check_in_type
  from db_ods_hr.clife_hr_ods_check_in
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and status =1
)b on a.old_man_check_in_id = b.old_man_check_in_id
left join db_ods_hr.clife_hr_ods_account e on b.old_man_id=e.old_man_id and e.symbol=1 and e.attributes=b.check_in_type and e.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.state=1 
  and a.hr_inform_type_id=2 
  and a.handle_type_id=2 
  and a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
)p 
left join db_dw_hr.clife_hr_dim_nursing_home m on p.sub_org_id=m.sub_org_id and m.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_old_man n on p.old_man_id=n.old_man_id and n.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dwd_service_record_info                  ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_service_record_info partition(part_date)
select
 a.increment_service_id     --服务主键
,a.old_man_id             --服务老人ID
,a.old_man_name           --长者姓名
,a.service_type_id        --服务类型id
,a.service_type_name      --服务类型id名
,a.service_project_id     --服务项目id
,a.service_project_name   --服务项目名称
,a.service_type           --服务类型
,a.cost                   --费用
,a.company                --单位
,a.remarks                --备注
,a.total_cost             --合计费用
,a.settlement_status      --结算状态
,a.service_status         --完成状态
,a.service_time           --服务时间
,a.suplier_id             --供应商id
,a.suplier_name           --供应商
,a.third_flag             --是否需要第三方
,a.old_man_check_in_id    --长者入住id
,a.pic_url                --服务图片
,a.service_object         --服务对象
,a.service_finish_time    --服务完成时间
,a.evaluation_status      --评价状态
,e.evaluate_time           --评价时间                
,a.valid_status           --订单有效性
,a.time_stage             --服务时间段
,a.is_accurate            --是否精确时间
,a.plan_id                --基础服务计划ID
,a.ignore_status          --忽略状态
,a.bed_id                 --床位id
,a.bed_name               --床位名称
,a.room_id                --房间id
,a.room_name              --房间名称
,a.floor_id               --楼层id
,a.floor_name             --楼层名称
,a.building_id            --楼栋id
,a.building_name          --楼栋名称
,a.service_housekeeper_ids    --服务管家ids
,b.service_keeper_names     --服务管家名称s
,a.create_time                --服务生成时间
,a.create_user_id             --创建人id
,a.create_user_name           --创建人名称
,a.update_time                --更新时间
,a.update_user_id             --更新人id
,a.update_user_name           --更新人名称
,c.nursin_homes_id       --养老院id（老人或者房间所属养老院）
,d.nursing_homes_name     --'养老院名称 '
,d.sub_org_id             --'养老机构id'
,d.sub_org_name           --'养老集团名称'
,d.org_id                 --'养老集团id'
,d.org_name               --'养老机构名称'     
,d.area_id                --'区id'        
,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from db_ods_hr.clife_hr_ods_service_record a
left join db_ods_hr.clife_hr_ods_basic_service_plan b on a.plan_id=b.plan_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_old_man c on a.old_man_id = c.old_man_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_nursing_home d on c.nursin_homes_id = d.nursing_homes_id and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
   select *
   from
   (
     select
          increment_service_id
         ,create_time as evaluate_time
         ,row_number()over(partition by increment_service_id order by create_time desc) as rank
     from db_ods_hr.clife_hr_ods_increment_evaluate 
     where part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and state=1 and evaluate_status=1
   )t
   where t.rank=1
)e on a.increment_service_id=e.increment_service_id
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


