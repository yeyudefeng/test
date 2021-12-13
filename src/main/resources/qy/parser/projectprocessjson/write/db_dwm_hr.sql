================================================================================
==========          clife_hr_dwm_market                               ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwm_market partition(part_date)
select 
 a.building_id                --   '养老院楼栋ID'  
,a.membership_adviser_id      --    '会籍顾问id'
,a.room_type                  --    '关注房型'
,a.product_id                 --    '产品id'
,a.sub_org_id                 --    '机构id'
,max(a.sub_org_name)  sub_org_name              --    '机构名称'
,max(a.org_id)        org_id             --    '集团id'
,max(a.org_name)      org_name             --    '集团名称'
,max(a.area_id)       area_id             --    '区域id'
,max(a.area_name)     area_name             --    '区域名称'
,max(a.card_id)       card_id             --     '卡id'
,max(a.card_name)     card_name             --     '卡名称'
,sum(a.deposit_amount)    deposit_amount         --     '下定额'
,sum(a.card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(a.cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(a.weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(a.alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(a.other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(a.receivable_amount) receivable_amount            --  '应收金额'
,sum(a.received_amount)   received_amount            --  '已收金额'
,sum(a.received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(a.received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(a.membership_amount)           membership_amount     --  '会籍费'
,sum(a.overdue_amount)              overdue_amount    --  '滞纳金'
,sum(a.received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(a.other_amount)                other_amount     --  '其他费用'
,sum(a.manager_amount)              manager_amount       --  '管理费'
,sum(a.trust_amount)                trust_amount      --  '托管费'
,sum(a.membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(a.card_amount)                 card_amount        --  '卡费'
,sum(a.card_manager_amount)         card_manager_amount     --  '卡管理费'
,sum(a.other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(a.transfor_to_amount)          transfor_to_amount      --  '转让费'
,sum(a.other_transfor_from_amount)  other_transfor_from_amount    --  '其他继承费'
,sum(a.transfor_from_amount)        transfor_from_amount       --  '继承费'
,sum(a.withdraw_amount)             withdraw_amount        --  '退会金额' 
,sum(a.today_deposit_amount)        today_deposit_amount       --  '今日下定额'
,sum(a.today_card_pay_amount)       today_card_pay_amount        --   '今日刷卡金额'
,sum(a.today_cash_pay_amount)       today_cash_pay_amount       --   '今日现金金额'
,sum(a.today_weixin_pay_amount)     today_weixin_pay_amount       --   '今日微信支付金额'
,sum(a.today_alipay_amount)         today_alipay_amount       --   '今日阿里支付金额'	
,sum(a.today_other_pay_amount)      today_other_pay_amount        --   '今日其他方式支付金额'
,sum(a.today_receivable_amount)     today_receivable_amount         --  '今日应收金额'
,sum(a.today_received_amount)       today_received_amount        --  '今日已收金额'
,regexp_replace(date_sub(current_date(),1),'-','') as part_date 
from 
(
select 
 t.building_id                --   '养老院楼栋ID'  
,t.membership_adviser_id      --    '会籍顾问id'
,t.room_type                  --    '关注房型'
,t.product_id                 --    '产品id'
,t.sub_org_id                 --    '机构id'
,t.sub_org_name               --    '机构名称'
,t.org_id                     --    '集团id'
,t.org_name                   --    '集团名称'
,t.area_id                    --    '区域id'
,t.area_name                  --    '区域名称'
,t.card_id                    --     '卡id'
,t.card_name                  --     '卡名称'
,t.deposit_amount             --     '下定额'
,case when t.pay_type_id=0 then t.received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when t.pay_type_id=1 then t.received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when t.pay_type_id=2 then t.received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when t.pay_type_id=3 then t.received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when t.pay_type_id=4 then t.received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,t.receivable_amount             --  '应收金额'
,t.received_amount               --  '已收金额'
,t.received_installment_amount   --  '已收分期金额'
,t.received_occupancy_amount     --  '已收占用费'
,t.membership_amount             --  '会籍费'
,t.overdue_amount                --  '滞纳金'
,t.received_overdue_amount       --  '已收滞纳金'
,t.other_amount                  --  '其他费用'
,t.manager_amount                --  '管理费'
,t.trust_amount                  --  '托管费'
,t.membership_discount_amount    --  '会籍优惠价'  
,t.card_amount                   --  '卡费'
,t.card_manager_amount           --  '卡管理费'
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.other_transfor_from_amount    --  '其他继承费'
,t.transfor_from_amount          --  '继承费'
,t.withdraw_amount               --  '退会金额' 
,case when  from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) then t.deposit_amount 
else null end as  today_deposit_amount             --  '今日下定额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) and t.pay_type_id=0 then t.received_amount  else null end as today_card_pay_amount            --   '今日刷卡金额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) and t.pay_type_id=1 then t.received_amount  else null end as today_cash_pay_amount            --   '今日现金金额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) and t.pay_type_id=2 then t.received_amount  else null end as today_weixin_pay_amount          --   '今日微信支付金额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) and t.pay_type_id=3 then t.received_amount  else null end as today_alipay_amount              --   '今日阿里支付金额'	
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) and t.pay_type_id=4 then t.received_amount  else null end as today_other_pay_amount           --   '今日其他方式支付金额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) then t.receivable_amount else null end  as today_receivable_amount          --  '今日应收金额'
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) then t.received_amount  else null end as today_received_amount            --  '今日已收金额'
from db_dw_hr.clife_hr_dwd_market t
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.building_id,a.membership_adviser_id,a.room_type,a.product_id,a.sub_org_id


================================================================================
==========          clife_hr_dwm_activity_manage_aggregation          ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwm_activity_manage_aggregation partition(part_date)
select
     t.activity_type        --活动类型
    ,t.nurse_home_id        --养老院id
    ,t.nursing_homes_name  --养老院名称
    ,t.sub_org_id         --机构id
    ,t.sub_org_name       --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.reserve_num --预定活动室次数
    ,t8.reserve_num_day --当日活动室预定次数
    ,t.template_cnt  --使用活动模板数量
    ,t1.template_num_day --当日使用活动模板数量	
    ,t.activity_num --已创建活动数
    ,t.apply_activity_cnt --已报名活动数
    ,t.host_activity_cnt --已举办活动数
    ,t1.activity_num_day --当日创建活动数	
    ,t1.apply_activity_num_day --当日报名活动数
    ,t2.host_activity_num_day --当日举办活动数
    ,t.apply_old_man_cnt --报名人次
    ,t.sign_old_man_cnt --签到人次
    ,t.confirm_old_man_cnt --到场人次
    ,t4.apply_old_man_num_day --当日报名人次	
    ,t6.sign_old_man_num_day --当日签到人次	
    ,t7.present_num_day --当日到场人次	
    ,t.activity_fee --活动费用
    ,t1.activity_fee_day  --当日活动费用
    ,t.good_evaluation_num     --活动评价好评数
    ,t.middle_evaluation_num   --活动评价中评数
    ,t.bad_evaluation_num      --活动评价差评数
    ,t.good_feedback_num       --反馈非常好数
    ,t.middle_feedback_num     --反馈较好数
    ,t.general_feedback_num    --反馈一般数
    ,t.bad_feedback_num        --反馈不理想数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       activity_type        --活动类型
      ,nurse_home_id        --养老院id
      ,max(nursing_homes_name) as nursing_homes_name    --养老院名称
      ,max(sub_org_id)         as sub_org_id       --机构id
      ,max(sub_org_name)       as sub_org_name     --机构名称
      ,max(org_id)             as org_id           --集团id
      ,max(org_name)           as org_name         --集团名称
      ,max(area_id)            as area_id          --区域id
      ,count(template_id)      as template_cnt     --使用活动模板数量
      ,count(activity_id)      as activity_num--已创建活动数
      ,sum(length(reserve_ids)) as reserve_num --预定活动室次数
      ,sum(case when apply_old_man_cnt is not null then 1 else 0 end) as apply_activity_cnt --已报名活动数
      ,count(activity_time) as host_activity_cnt --已举办活动数
      ,sum(apply_old_man_cnt) as apply_old_man_cnt --报名人次
      ,sum(sign_old_man_cnt)  as sign_old_man_cnt --签到人次
      ,sum(confirm_old_man_cnt)  as confirm_old_man_cnt --到场人次
      ,sum(activity_fee) as activity_fee --活动费用
      ,sum(good_evaluation_num)   as good_evaluation_num     --活动评价好评数
      ,sum(middle_evaluation_num) as middle_evaluation_num   --活动评价中评数
      ,sum(bad_evaluation_num)    as bad_evaluation_num      --活动评价差评数
      ,sum(good_feedback_num)     as good_feedback_num       --反馈非常好数
      ,sum(middle_feedback_num)   as middle_feedback_num     --反馈较好数
      ,sum(general_feedback_num)  as general_feedback_num    --反馈一般数
      ,sum(bad_feedback_num)      as bad_feedback_num        --反馈不理想数
  from db_dw_hr.clife_hr_dwd_activity_manage_info
  where part_date = regexp_replace(date_sub(current_date(),1),'-','')
  group by activity_type
          ,nurse_home_id 
)t
left join
(
  select
      vl.activity_type
     ,vl.nurse_home_id
     ,vl.activity_create_time
     ,vs.template_num_day --当日使用活动模板数量
     ,vs.activity_num_day --当日创建活动数
     ,vs.apply_activity_num_day --当日报名活动数
     ,vs.activity_fee_day  --当日活动费用
  from
  (
    select
         activity_type
        ,nurse_home_id
        ,activity_create_time
        ,row_number() over(partition by activity_type,nurse_home_id order by activity_create_time desc) as rank
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    where part_date = regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (--（根据养老院id，活动类型分区，activity_create_time排序，取最新一条）
    select
         activity_type
        ,nurse_home_id 
        ,substr(activity_create_time,1,10) as activity_create_time
        ,sum(template_id) as template_num_day --当日使用活动模板数量
        ,sum(activity_id) as activity_num_day --当日创建活动数
        ,sum(case when apply_old_man_cnt is not null then 1 else 0 end) as apply_activity_num_day --当日报名活动数
        ,sum(activity_fee) as activity_fee_day  --当日活动费用
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    where part_date = regexp_replace(date_sub(current_date(),1),'-','')
    group by activity_type
            ,nurse_home_id 
            ,substr(activity_create_time,1,10)
  )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.activity_create_time,1,10) = vs.activity_create_time
  where vl.rank=1
)t1 on t.activity_type = t1.activity_type and t.nurse_home_id = t1.nurse_home_id
left join
(
 select
      vl.activity_type
     ,vl.nurse_home_id
     ,vl.activity_time
     ,vs.host_activity_num_day --当日举办活动数
  from
  (
    select
         activity_type,nurse_home_id,activity_time
        ,row_number() over(partition by activity_type,nurse_home_id order by activity_time desc) as rank
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    where part_date = regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (--（根据养老院id，活动类型分区，activity_time 排序，取最新一条）
    select
         activity_type
        ,nurse_home_id 
        ,substr(activity_time,1,10) as activity_time
        ,count(activity_id) as host_activity_num_day --当日举办活动数
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    where part_date = regexp_replace(date_sub(current_date(),1),'-','')
    group by activity_type
            ,nurse_home_id 
            ,substr(activity_time,1,10)
  )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.activity_time,1,10) = vs.activity_time
  where vl.rank=1
)t2 on t.activity_type = t2.activity_type and t.nurse_home_id = t2.nurse_home_id
left join
(--根据养老院id，活动类型分区，apply_times 排序，取最新一条
  select
       vl.activity_type
      ,vl.nurse_home_id
      ,vl.apply_time
      ,vs.apply_old_man_num_day --当日报名人次
  from
  (
     select
          activity_type
         ,nurse_home_id
         ,ats.apply_time
         ,row_number()over(partition by activity_type,nurse_home_id order by ats.apply_time desc) as rank_apply
     from
     (
       select 
            activity_type
           ,nurse_home_id
           ,apply_times
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d lateral view explode(split(d.apply_times,';'))ats as apply_time
  )vl
  left join
  (
     select
          activity_type
         ,nurse_home_id
         ,substr(apply_time,1,10) as apply_time
         ,count(1) as apply_old_man_num_day --当日报名人次
     from
     ( 
       select 
            activity_type
           ,nurse_home_id
           ,apply_times
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d lateral view explode(split(d.apply_times,';'))ats as apply_time
     group by activity_type
             ,nurse_home_id
             ,substr(apply_time,1,10)
   )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.apply_time,1,10) = vs.apply_time
   where vl.rank_apply = 1
)t4 on t.activity_type = t4.activity_type and t.nurse_home_id = t4.nurse_home_id
left join
(--根据养老院id，活动类型分区，reserve_times 排序，取最新一条
  select
       vl.activity_type
      ,vl.nurse_home_id
      ,vl.reserve_time
      ,vs.reserve_num_day --当日活动室预定次数
  from
  (
     select
          activity_type
         ,nurse_home_id
         ,reserve_time
         ,reserve_id
         ,row_number()over(partition by activity_type,nurse_home_id order by reserve_time desc) as rank_reserve
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.reserve_time
           ,temp.reserve_id
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.reserve_times is null,'',a.reserve_times),if(a.reserve_ids is null,'',a.reserve_ids)) temp as reserve_time,reserve_id
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
  )vl
  left join
  (
     select
          d.activity_type
         ,d.nurse_home_id
         ,substr(d.reserve_time,1,10) as reserve_time
         ,count(d.reserve_id) as reserve_num_day --当日活动室预定次数
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.reserve_time
           ,temp.reserve_id
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.reserve_times is null,'',a.reserve_times),if(a.reserve_ids is null,'',a.reserve_ids)) temp as reserve_time,reserve_id
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
     group by activity_type
             ,nurse_home_id
             ,substr(d.reserve_time,1,10)
   )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.reserve_time,1,10) = vs.reserve_time
   where vl.rank_reserve = 1
)t8 on t.activity_type = t8.activity_type and t.nurse_home_id = t8.nurse_home_id
left join
(--根据养老院id，活动类型分区，reserve_times 排序，取最新一条
  select
       vl.activity_type
      ,vl.nurse_home_id
      ,vl.sign_time
      ,vs.sign_old_man_num_day --当日签到人次
  from
  (
     select
          activity_type
         ,nurse_home_id
         ,sign_time
         ,row_number()over(partition by activity_type,nurse_home_id order by sign_time desc) as rank_sign
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.sign_time
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.sign_times is null,'',a.sign_times)) temp as sign_time
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
  )vl
  left join
  (
     select
          activity_type
         ,nurse_home_id
         ,substr(d.sign_time,1,10) as sign_time
         ,count(1) as sign_old_man_num_day --当日签到人次
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.sign_time
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.sign_times is null,'',a.sign_times)) temp as sign_time
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
     group by activity_type
             ,nurse_home_id
             ,substr(d.sign_time,1,10)
   )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.sign_time,1,10) = vs.sign_time
   where vl.rank_sign = 1
)t6 on t.activity_type = t6.activity_type and t.nurse_home_id = t6.nurse_home_id
left join
(--根据养老院id，活动类型分区，confirm_times 排序，取最新一条
  select
       vl.activity_type
      ,vl.nurse_home_id
      ,vl.confirm_time
      ,vs.present_num_day --当日到场人次
  from
  (
     select
          activity_type
         ,nurse_home_id
         ,d.confirm_time
         ,row_number()over(partition by activity_type,nurse_home_id order by d.confirm_time desc) as rank_confirm
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.confirm_time
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.confirm_times is null,'',a.confirm_times)) temp as confirm_time
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
  )vl
  left join
  (
     select
          activity_type
         ,nurse_home_id
         ,substr(d.confirm_time,1,10) as confirm_time
         ,count(1) as present_num_day --当日到场人次
     from
     (
       select 
            a.activity_type
           ,a.nurse_home_id
           ,temp.confirm_time
       from db_dw_hr.clife_hr_dwd_activity_manage_info a
       lateral view explodemulti(if(a.confirm_times is null,'',a.confirm_times)) temp as confirm_time
       where part_date = regexp_replace(date_sub(current_date(),1),'-','')
     )d
     group by activity_type
             ,nurse_home_id
             ,substr(d.confirm_time,1,10)
   )vs on vl.activity_type = vs.activity_type and vl.nurse_home_id = vs.nurse_home_id and substr(vl.confirm_time,1,10) = vs.confirm_time
   where vl.rank_confirm = 1
)t7 on t.activity_type = t7.activity_type and t.nurse_home_id = t7.nurse_home_id


================================================================================
==========          clife_hr_dwm_recharge_consumption_aggregation     ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwm_recharge_consumption_aggregation partition(part_date)
select
     t.account_id          --账户id
    ,t.fee_iteam_id        --费用项目id
    ,t.account_name        --账户名称
    ,t.attributes          --账户属性
    ,t.fee_iteam_name      --费用项目名称
    ,t.fee_type_id         --费用类型id
    ,t.fee_type_name       --费用类型名称
    ,t.old_man_id          --老人id(普通账户)
    ,t.member_id           --权益人id(会员账户)
    ,t.old_man_name        --姓名
    ,t.sex                 --性别
    ,t.age                 --年龄
    ,t.nursing_homes_id    --养老院id
    ,t.nursing_homes_name  --养老院名称
    ,t.sub_org_id          --养老机构id
    ,t.sub_org_name        --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.check_in_payment_num      --入住缴费次数
    ,t.check_in_payment_amount   --入住缴费金额
    ,t.recharge_num              --账户充值次数
    ,t.recharge_amount           --账户充值金额
    ,t.member_recharge_num       --会员充值次数
    ,t.member_recharge_amount    --会员充值金额
    ,t.consume_num               --账户消费次数
    ,t.consume_amount            --账户消费金额
    ,t.member_consume_num        --会员消费次数
    ,t.member_consume_amount     --会员消费金额
    ,t.countermand_amount        --退住退费金额
    ,t2.check_in_payment_cnt_day    --当日入住缴费次数
    ,t2.check_in_payment_amount_day --当日入住缴费金额
    ,t3.recharge_cnt_day            --当日账户充值次数
    ,t3.recharge_amount_day         --当日账户充值金额
    ,t4.member_recharge_cnt_day     --当日会员充值次数
    ,t4.member_recharge_amount_day  --当日会员充值金额
    ,t5.consume_cnt_day             --当日账户消费次数
    ,t5.consume_amount_day          --当日账户消费金额
    ,t6.member_consume_cnt_day      --当日会员消费金额
    ,t6.member_consume_amount_day   --当日会员消费次数
    ,t7.countermand_amount_day      --当日退住退费金额    
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       a.account_id               --账户id
      ,a.fee_iteam_id             --费用项目id
      ,max(b.account_name)        as account_name      --账户名称
      ,max(b.attributes)          as attributes        --账户属性
      ,max(a.fee_iteam_name)      as fee_iteam_name    --费用项目名称
      ,max(a.fee_type_id)         as fee_type_id       --费用类型id
      ,max(a.fee_type_name)       as fee_type_name     --费用类型名称
      ,max(a.old_man_id)          as old_man_id        --老人id(普通账户)
      ,max(a.member_id)           as member_id         --权益人id(会员账户)
      ,max(a.old_man_name)        as old_man_name      --姓名
      ,max(a.sex)                 as sex               --性别
      ,max(a.age)                 as age               --年龄
      ,max(a.nursing_homes_id)    as nursing_homes_id  --养老院id
      ,max(a.nursing_homes_name)  as nursing_homes_name--养老院名称
      ,max(a.sub_org_id)          as sub_org_id        --养老机构id
      ,max(a.sub_org_name)        as sub_org_name      --机构名称
      ,max(a.org_id)              as org_id            --集团id
      ,max(a.org_name)            as org_name          --集团名称
      ,max(a.area_id)             as area_id           --区域id
      ,sum(case when behavior_type='入住缴费' then 1 else 0 end)      as check_in_payment_num      --入住缴费次数
      ,sum(case when behavior_type='入住缴费' then amount else 0 end) as check_in_payment_amount   --入住缴费金额
      ,sum(case when behavior_type='账户充值' then 1 else 0 end)      as recharge_num              --账户充值次数
      ,sum(case when behavior_type='账户充值' then amount else 0 end) as recharge_amount           --账户充值金额
      ,sum(case when behavior_type='会员充值' then 1 else 0 end)      as member_recharge_num       --会员充值次数
      ,sum(case when behavior_type='会员充值' then amount else 0 end) as member_recharge_amount    --会员充值金额
      ,sum(case when behavior_type='账户消费' then 1 else 0 end)      as consume_num               --账户消费次数
      ,sum(case when behavior_type='账户消费' then amount else 0 end) as consume_amount            --账户消费金额
      ,sum(case when behavior_type='会员消费' then 1 else 0 end)      as member_consume_num        --会员消费金额
      ,sum(case when behavior_type='会员消费' then amount else 0 end) as member_consume_amount     --会员消费次数
      ,sum(case when behavior_type='退住退费' then amount else 0 end) as countermand_amount        --退住退费金额
  from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
  left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by a.account_id
          ,a.fee_iteam_id
)t
left join
(
  select
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.check_in_payment_time
      ,vs.check_in_payment_cnt_day    --当日入住缴费次数
      ,vs.check_in_payment_amount_day --当日入住缴费金额
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='入住缴费'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,count(1)    as check_in_payment_cnt_day    --当日入住缴费次数
       ,sum(amount) as check_in_payment_amount_day --当日入住缴费金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='入住缴费'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t2 on t.account_id = t2.account_id and t.fee_iteam_id = t2.fee_iteam_id
left join
(
  select
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.check_in_payment_time
      ,vs.recharge_cnt_day    --当日账户充值次数
      ,vs.recharge_amount_day --当日账户充值金额
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='账户充值'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,count(1)    as recharge_cnt_day    --当日账户充值次数
       ,sum(amount) as recharge_amount_day --当日账户充值金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='账户充值'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t3 on t.account_id = t3.account_id and t.fee_iteam_id = t3.fee_iteam_id
left join
(
  select
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.check_in_payment_time
      ,vs.member_recharge_cnt_day    --当日会员充值次数
      ,vs.member_recharge_amount_day --当日会员充值金额
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='会员充值'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,count(1)    as  member_recharge_cnt_day    --当日会员充值次数
       ,sum(amount) as  member_recharge_amount_day --当日会员充值金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='会员充值'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t4 on t.account_id = t4.account_id and t.fee_iteam_id = t4.fee_iteam_id
left join
(
  select
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.check_in_payment_time
      ,vs.consume_cnt_day    --当日账户消费次数
      ,vs.consume_amount_day --当日账户消费金额
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='账户消费'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,count(1)    as  consume_cnt_day    --当日账户消费次数
       ,sum(amount) as  consume_amount_day --当日账户消费金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='账户消费'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t5 on t.account_id = t5.account_id and t.fee_iteam_id = t5.fee_iteam_id
left join
(
  select 
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.check_in_payment_time
      ,vs.member_consume_cnt_day    --当日账户消费次数
      ,vs.member_consume_amount_day --当日账户消费金额
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='会员消费'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,count(1)    as  member_consume_cnt_day    --当日账户消费次数
       ,sum(amount) as  member_consume_amount_day --当日账户消费金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='会员消费'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t6 on t.account_id = t6.account_id and t.fee_iteam_id = t6.fee_iteam_id
left join
(
  select
       vl.account_id
      ,vl.fee_iteam_id
      ,vl.recharge_consumption_time
      ,vs.countermand_amount_day
      ,vs.check_in_payment_time
  from
  (
    select
         account_id
        ,fee_iteam_id
        ,recharge_consumption_time
        ,row_number()over(partition by d.account_id,d.fee_iteam_id order by d.recharge_consumption_time desc) as rank 
	from
    (
      select *
      from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
        and a.behavior_type='退住退费'
   )d
  )vl
   left join
  (
   select
        account_id
       ,fee_iteam_id
       ,sum(amount) as  countermand_amount_day --当日退住退费金额
       ,substr(d.recharge_consumption_time,1,10) as check_in_payment_time
   from
   (
     select *
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
       and a.behavior_type='退住退费'
   )d
   group by account_id
           ,fee_iteam_id
           ,substr(d.recharge_consumption_time,1,10)
   ) vs on vl.account_id = vs.account_id and vl.fee_iteam_id = vs.fee_iteam_id and substr(vl.recharge_consumption_time,1,10) = vs.check_in_payment_time
   where vl.rank = 1
)t7 on t.account_id = t7.account_id and t.fee_iteam_id = t7.fee_iteam_id


================================================================================
==========          clife_hr_dwm_reside_manage_aggregation            ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwm_reside_manage_aggregation partition(part_date)
select
     t.old_man_id           --老人id
    ,t.floor_id             --楼层id
    ,t.building_id          --楼栋id
    ,t.nursin_homes_id      --养老院id
    ,t.nursing_homes_name   --养老院名称
    ,t.sub_org_id           --机构id
    ,t.sub_org_name         --机构名称
    ,t.org_id               --集团id
    ,t.org_name             --集团名称
    ,t.area_id              --区域id
    ,t.old_man_name         --老人姓名
    ,t.sex                  --老人性别
    ,t.age                  --老人年龄
    ,t.check_in_num         --老人入住次数
    ,t.countermand_num      --老人退住次数
    ,t.bed_amount           --楼层床位数
    ,t.room_amount          --楼层房间数
    ,t.relatives_names      --探访人姓名
    ,t.visit_times          --探访时间	
    ,t.refund_amount        --当天请假退费金额
    ,t.leave_reducation_fee --当天请假减免费用
    ,t.leave_day_cnt        --当日请假次数
    ,length(t.relatives_names) as relatives_num --探访人数
    ,t1.leave_time           --请假时长
    ,t1.sick_leave_num       --病假次数
    ,t1.casual_leave_num     --事假次数
    ,t1.other_leave_num      --其他假次数
    ,t1.leave_num            --请假次数
    ,up.reside_status        --老人居住状态（在住或者退住）
    ,up.room_id              --在住房间id
    ,up.bed_id               --在住床位id
    ,t3.leave_time_day       --当日请假时长
    ,t3.sick_leave_num_day   --当日病假次数
    ,t3.casual_leave_num_day --当日事假次数
    ,t3.other_leave_num_day  --当日其他假次数
    ,t6.visit_day_cnt        --当日探访人数
    ,t6.visit_time           --当日探访时长
    ,t2.countermand_cnt_day  --当日退住次数
    ,t5.reside_cnt_day       --当日入住次数
    ,null as contract_time_1_num        --录入合同数量（1年期）  
    ,null as contract_time_2_num        --录入合同数量（2年期）  
    ,null as contract_time_3_num        --录入合同数量（3年期）  
    ,null as contract_time_4_num        --录入合同数量（4年期）  
    ,null as contract_time_5_num        --录入合同数量（5年期）  
    ,null as contract_time_surpass_5_num--录入合同数量（5年以上）
    ,null as contract_time_1_num_day        --当日录入合同数量（1年期）  
    ,null as contract_time_2_num_day        --当日录入合同数量（2年期）  
    ,null as contract_time_3_num_day        --当日录入合同数量（3年期）  
    ,null as contract_time_4_num_day        --当日录入合同数量（4年期）  
    ,null as contract_time_5_num_day        --当日录入合同数量（5年期）  
    ,null as contract_time_surpass_5_num_day--当日录入合同数量（5年以上）
    ,reside_status_list
    ,room_id_list
    ,bed_id_list
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       a.old_man_id         --老人id
      ,b.floor_id           --楼层id
      ,max(b.building_id)        as building_id          --楼栋id
      ,max(a.nursin_homes_id)    as nursin_homes_id      --养老院id
      ,max(a.nursing_homes_name) as nursing_homes_name   --养老院名称
      ,max(a.sub_org_id)         as sub_org_id           --机构id
      ,max(a.sub_org_name)       as sub_org_name         --机构名称
      ,max(a.org_id)             as org_id               --集团id
      ,max(a.org_name)           as org_name             --集团名称
      ,max(a.area_id)            as area_id              --区域id
      ,max(a.old_man_name)       as old_man_name         --老人姓名
      ,max(a.sex)                as sex                  --老人性别
      ,max(a.age)                as age                  --老人年龄
      ,sum(case when reside_status = 3 then 1 else 0 end )as check_in_num        --老人入住次数
      ,sum(case when reside_status = 4 then 1 else 0 end) as countermand_num  --老人退住次数
      ,count(b.bed_amount)       as bed_amount      --楼层床位数
      ,count(a.room_id)          as room_amount     --楼层房间数
      ,max(relatives_names)      as relatives_names --探访人姓名
      ,max(visit_times)          as visit_times   --探访时间
      ,concat_ws(';',collect_list(cast(a.reside_status as string))) as reside_status_list
      ,concat_ws(';',collect_list(cast(a.room_id as string)))       as room_id_list
      ,concat_ws(';',collect_list(cast(a.bed_id  as string)))       as bed_id_list
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_1_num          --录入合同数量（1年期）  
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_2_num          --录入合同数量（2年期）  
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_3_num          --录入合同数量（3年期）  
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_4_num          --录入合同数量（4年期）  
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_5_num          --录入合同数量（5年期）  
      --,sum(case when (合同到期时间-开始时间) then 1 else 0 end ) as contract_time_surpass_5_num  --录入合同数量（5年以上）
      ,sum(refund_amount)        as refund_amount        --当天请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当天请假减免费用
      ,sum(leave_day_cnt)        as leave_day_cnt         --当日请假次数
  from db_dw_hr.clife_hr_dwd_reside_manage_info a
  left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by a.old_man_id
            ,b.floor_id 
)t
left join
(
  select *
  from
  (
    select a.old_man_id,b.floor_id,a.reside_status,a.room_id,a.bed_id
        ,row_number()over(partition by a.old_man_id,b.floor_id order by a.contract_entry_time desc) as rank
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )c where c.rank=1
)up on t.old_man_id = up.old_man_id and t.floor_id = up.floor_id
left join
(
    select
       tmp.old_man_id
      ,b.floor_id
      ,sum(tmp.leave_time) as leave_time  --请假时长
      ,sum(case when tmp.leave_type = 0 then 1 else 0 end) as sick_leave_num   --病假次数
      ,sum(case when tmp.leave_type = 1 then 1 else 0 end) as casual_leave_num --事假次数
      ,sum(case when tmp.leave_type = 2 then 1 else 0 end) as other_leave_num  --其他假次数
      ,count(1) as leave_num --请假次数
  from
  (
    select
         a.old_man_check_in_id
        ,a.old_man_id
        ,a.room_id
        ,ip.leave_time
        ,ip.leave_type
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    lateral view explodemulti(if(a.leave_times is null,'',a.leave_times),if(a.leave_types is null,'',a.leave_types)) ip as leave_time,leave_type 
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )tmp
  left join db_dw_hr.clife_hr_dim_location_room b on tmp.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by tmp.old_man_id
          ,b.floor_id
)t1 on t.old_man_id = t1.old_man_id and t.floor_id = t1.floor_id
left join
(
  select
       vl.old_man_id
      ,vl.floor_id
      ,vl.countermand_time
      ,vs.countermand_cnt_day --当日退住次数
  from
  (
    select  
        a.old_man_id,b.floor_id,countermand_time
        ,row_number()over(partition by a.old_man_id,b.floor_id order by a.countermand_time desc) as rank
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (
    select
        a.old_man_id
       ,b.floor_id
       ,count(1) as countermand_cnt_day --当日退住次数
       ,substr(a.countermand_time,1,10) as countermand_time
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by a.old_man_id
            ,b.floor_id
            ,substr(a.countermand_time,1,10)
  )vs on vl.old_man_id = vs.old_man_id and vl.floor_id = vs.floor_id and vs.countermand_time = substr(vl.countermand_time,1,10)
  where vl.rank=1
)t2 on t.old_man_id = t2.old_man_id and t.floor_id = t2.floor_id
left join
(
  select
       vd.old_man_id
      ,vd.floor_id
      ,vd.leave_create_time
      ,e.sick_leave_num_day   --当日病假次数
      ,e.casual_leave_num_day --当日事假次数
      ,e.other_leave_num_day  --当日其他假次数
      ,e.leave_time_day  --当日请假时长
  from
  (
    select
         tmp.old_man_id
        ,b.floor_id
        ,tmp.leave_create_time
        ,row_number()over(partition by tmp.old_man_id,b.floor_id order by tmp.leave_create_time desc) as rank
    from
    (
      select 
           a.old_man_id
          ,a.room_id
          ,ip.leave_create_time
          ,ip.leave_time
          ,ip.leave_type
      from db_dw_hr.clife_hr_dwd_reside_manage_info a
      lateral view explodemulti(if(a.leave_times is null,'',a.leave_times),if(a.leave_types is null,'',a.leave_types),if(a.leave_create_times is null,'',a.leave_create_times)) ip as leave_time,leave_type,leave_create_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )tmp
    left join db_dw_hr.clife_hr_dim_location_room b on tmp.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vd
  left join 
  (
    select
         tmp.old_man_id
        ,b.floor_id
        ,sum(tmp.leave_time) as leave_time_day  --当日请假时长
        ,sum(case when tmp.leave_type = 0 then 1 else 0 end) as sick_leave_num_day   --当日病假次数
        ,sum(case when tmp.leave_type = 1 then 1 else 0 end) as casual_leave_num_day --当日事假次数
        ,sum(case when tmp.leave_type = 2 then 1 else 0 end) as other_leave_num_day  --当日其他假次数
        ,substr(tmp.leave_create_time,1,10) as leave_create_time
    from
    (
      select 
           a.old_man_id
          ,a.room_id
          ,ip.leave_create_time
          ,ip.leave_time
          ,ip.leave_type
      from db_dw_hr.clife_hr_dwd_reside_manage_info a
      lateral view explodemulti(if(a.leave_times is null,'',a.leave_times),if(a.leave_types is null,'',a.leave_types),if(a.leave_create_times is null,'',a.leave_create_times)) ip as leave_time,leave_type,leave_create_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )tmp
    left join db_dw_hr.clife_hr_dim_location_room b on tmp.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by tmp.old_man_id
            ,b.floor_id
            ,substr(tmp.leave_create_time,1,10)
  )e on vd.old_man_id = e.old_man_id and vd.floor_id = e.floor_id and substr(vd.leave_create_time,1,10) = e.leave_create_time
  where vd.rank = 1
)t3 on t.old_man_id = t3.old_man_id and t.floor_id = t3.floor_id
left join 
(
  select 
       vl.old_man_id
      ,vl.floor_id
      ,vl.reside_time
      ,vs.reside_cnt_day --当日入住次数
  from
  (
    select  
         a.old_man_id,b.floor_id,a.reside_status,a.room_id,a.bed_id,a.reside_time
        ,row_number()over(partition by a.old_man_id,b.floor_id order by a.reside_time desc) as rank 
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join 
  (
    select
         a.old_man_id
        ,b.floor_id
        ,count(1) as reside_cnt_day --当日入住次数
        ,substr(a.reside_time,1,10) as reside_time
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by a.old_man_id
            ,b.floor_id
            ,substr(a.reside_time,1,10)
   )vs on vl.old_man_id = vs.old_man_id and vl.floor_id = vs.floor_id and vs.reside_time = substr(vl.reside_time,1,10)
   where vl.rank=1
)t5 on t.old_man_id = t5.old_man_id and t.floor_id = t5.floor_id
left join
(
  select
       vl.old_man_id
      ,vl.floor_id
      ,vl.visit_time
      ,vs.visit_day_cnt  --当日探访人数
  from
  (
    select
         tmp.old_man_id
        ,b.floor_id
        ,tmp.visit_time
        ,row_number()over(partition by tmp.old_man_id,b.floor_id order by tmp.visit_time desc) as rank_visit 
	from
    (
      select 
           a.old_man_id
          ,a.room_id
          ,ip.relatives_name
          ,ip.visit_time
      from db_dw_hr.clife_hr_dwd_reside_manage_info a
      lateral view explodemulti(if(a.relatives_names is null,'',a.relatives_names),if(a.visit_times is null,'',a.visit_times)) ip as relatives_name,visit_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )tmp
    left join db_dw_hr.clife_hr_dim_location_room b on tmp.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (
    select
         tmp.old_man_id
        ,b.floor_id
        ,count(1) as visit_day_cnt  --当日探访人数
        ,substr(tmp.visit_time,1,10) as visit_time
    from
    (
      select 
           a.old_man_id
          ,a.room_id
          ,ip.relatives_name
          ,ip.visit_time
      from db_dw_hr.clife_hr_dwd_reside_manage_info a
      lateral view explodemulti(if(a.relatives_names is null,'',a.relatives_names),if(a.visit_times is null,'',a.visit_times)) ip as relatives_name,visit_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )tmp
    left join db_dw_hr.clife_hr_dim_location_room b on tmp.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by tmp.old_man_id
            ,b.floor_id
            ,substr(tmp.visit_time,1,10)
   ) vs on vl.old_man_id = vs.old_man_id and vl.floor_id = vs.floor_id and substr(vl.visit_time ,1,10) = vs.visit_time
   where vl.rank_visit = 1
)t6 on t.old_man_id = t6.old_man_id and t.floor_id = t6.floor_id


================================================================================
==========          clife_hr_dwm_service_record_aggregation           ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwm_service_record_aggregation partition(part_date)
select
     t.nursin_homes_id    --养老院id
    ,t.service_type       --服务类型（增值or基础）
    ,t.service_type_id    --服务类型id
    ,t.service_project_id --服务项目id
    ,t.nursing_homes_name --养老院名称
    ,t.sub_org_id         --机构id
    ,t.sub_org_name       --机构名称
    ,t.org_id             --集团id
    ,t.org_name           --集团名称
    ,t.cost               --服务总费用
    ,t.service_cnt        --服务数
    ,t.already_settle_service_num     --已结算服务数
    ,t.no_settle_service_num          --未结算服务数
    ,t.wait_complete_service_num      --待完成服务数
    ,t.complete_service_num           --已完成服务数
    ,t.evaluation_service_num         --已评价服务数
    ,t.evaluation_wait_num            --待评价服务数
    ,t.evaluation_good_num            --好评数
    ,t.evaluation_middle_num          --中评数
    ,t.evaluation_bad_num             --差评数
    ,t2.service_cnt_day           --当日服务数(create_time)
    ,t3.complete_service_cnt_day  --当日完成服务数(service_finish_time)
    ,t4.evaluation_cnt_day        --当日评价数(评价时间 evaluate_times)
    ,t4.evaluation_good_num_day   --当日好评数(评价时间 evaluate_times)
    ,t4.evaluation_middle_num_day --当日中评数(评价时间 evaluate_times)
    ,t4.evaluation_bad_num_day    --当日差评数(评价时间 evaluate_times)
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       nursin_homes_id  --养老院id
      ,service_type     --服务类型（增值or基础）
      ,service_type_id  --服务类型id
      ,service_project_id --服务项目id
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id--机构id
      ,max(sub_org_name)       as sub_org_name--机构名称
      ,max(org_id)             as org_id--集团id
      ,max(org_name)           as org_name--集团名称
      ,sum(cost)               as cost  --服务总费用
      ,count(1) as  service_cnt --服务数
      ,sum(case when settlement_status =  2 then 1 else 0 end ) as  already_settle_service_num     --已结算服务数
      ,sum(case when settlement_status =  3 then 1 else 0 end ) as  no_settle_service_num          --未结算服务数
      ,sum(case when service_status = 1  then 1 else 0 end )    as  wait_complete_service_num      --待完成服务数
      ,sum(case when service_status = 2  then 1 else 0 end )    as  complete_service_num           --已完成服务数
      ,sum(case when service_status = 3  then 1 else 0 end )    as  evaluation_service_num         --已评价服务数
      ,sum(case when evaluation_status = 0  then 1 else 0 end ) as  evaluation_wait_num            --待评价服务数
      ,sum(case when evaluation_status = 1  then 1 else 0 end ) as  evaluation_good_num            --好评数
      ,sum(case when evaluation_status = 2  then 1 else 0 end ) as  evaluation_middle_num          --中评数
      ,sum(case when evaluation_status = 3  then 1 else 0 end ) as  evaluation_bad_num             --差评数
  from db_dw_hr.clife_hr_dwd_service_record_info a
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
          ,service_type       
          ,service_type_id    
          ,service_project_id 
)t
left join
(--根据养老院id，服务类型，,服务类型id,服务项目id,create_time 排序，取最新一条
  select
       vl.nursin_homes_id  --养老院id
      ,vl.service_type     --服务类型（增值or基础）
      ,vl.service_type_id  --服务类型id
      ,vl.service_project_id --服务项目id
      ,vl.create_time
      ,vs.service_cnt_day --当日服务数
  from
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,create_time  --创建时间
         ,row_number()over(partition by nursin_homes_id,service_type,service_type_id,service_project_id order by create_time desc) as rank
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,count(1) as service_cnt_day --当日服务数
         ,substr(create_time,1,10) as service_time
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
     group by nursin_homes_id
             ,service_type   
             ,service_type_id
             ,service_project_id
             ,substr(create_time,1,10)
   )vs on vl.nursin_homes_id = vs.nursin_homes_id and vl.service_type = vs.service_type and substr(vl.create_time,1,10) = vs.service_time and vl.service_type_id = vs.service_type_id and vl.service_project_id = vs.service_project_id 
   where vl.rank = 1
)t2 on t.nursin_homes_id = t2.nursin_homes_id and t.service_type = t2.service_type and t.service_type_id = t2.service_type_id and t.service_project_id = t2.service_project_id 
left join
(----根据养老院id，服务类型，,服务类型id,服务项目id,service_finish_time 排序，取最新一条
  select
       vl.nursin_homes_id  --养老院id
      ,vl.service_type     --服务类型（增值or基础）
      ,vl.service_type_id  --服务类型id
      ,vl.service_project_id --服务项目id
      ,vl.service_finish_time
      ,vs.complete_service_cnt_day --当日完成服务数
  from
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,service_finish_time
         ,row_number()over(partition by nursin_homes_id,service_type,service_type_id,service_project_id order by service_finish_time desc) as rank
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,sum(case when service_status = 2 or service_status = 3  then 1 else 0 end) as complete_service_cnt_day --当日完成服务数
         ,substr(service_finish_time,1,10) as service_time
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
     group by nursin_homes_id
             ,service_type   
             ,service_type_id
             ,service_project_id
             ,substr(service_finish_time,1,10)
   )vs on vl.nursin_homes_id = vs.nursin_homes_id and vl.service_type = vs.service_type and substr(vl.service_finish_time,1,10) = vs.service_time and vl.service_type_id = vs.service_type_id and vl.service_project_id = vs.service_project_id 
   where vl.rank = 1
)t3 on t.nursin_homes_id = t3.nursin_homes_id and t.service_type = t3.service_type and t.service_type_id = t3.service_type_id and t.service_project_id = t3.service_project_id 
left join
(----根据养老院id，服务类型，,服务类型id,服务项目id,evaluate_times 排序，取最新一条
  select
       vl.nursin_homes_id  --养老院id
      ,vl.service_type     --服务类型（增值or基础）
      ,vl.service_type_id  --服务类型id
      ,vl.service_project_id --服务项目id
      ,vl.evaluate_time
      ,vs.evaluation_cnt_day --当日评价数
      ,vs.evaluation_good_num_day   --当日好评数
      ,vs.evaluation_middle_num_day --当日中评数
      ,vs.evaluation_bad_num_day    --当日差评数
  from
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,evaluate_time
         ,row_number()over(partition by nursin_homes_id,service_type,service_type_id,service_project_id order by evaluate_time desc) as rank
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )vl
  left join
  (
     select
          nursin_homes_id  --养老院id
         ,service_type     --服务类型（增值or基础）
         ,service_type_id  --服务类型id
         ,service_project_id --服务项目id
         ,count(1) as evaluation_cnt_day --当日评价数
         ,sum(case when evaluation_status = 1 then 1 else 0 end) as evaluation_good_num_day   --当日好评数
         ,sum(case when evaluation_status = 2 then 1 else 0 end) as evaluation_middle_num_day --当日中评数
         ,sum(case when evaluation_status = 3 then 1 else 0 end) as evaluation_bad_num_day    --当日差评数
         ,substr(evaluate_time,1,10) as service_time
     from db_dw_hr.clife_hr_dwd_service_record_info
	 where part_date=regexp_replace(date_sub(current_date(),1),'-','')
     group by nursin_homes_id
             ,service_type   
             ,service_type_id
             ,service_project_id
             ,substr(evaluate_time,1,10)
   )vs on vl.nursin_homes_id = vs.nursin_homes_id and vl.service_type = vs.service_type and substr(vl.evaluate_time,1,10) = vs.service_time and vl.service_type_id = vs.service_type_id and vl.service_project_id = vs.service_project_id 
   where vl.rank = 1
)t4 on t.nursin_homes_id = t4.nursin_homes_id and t.service_type = t4.service_type and t.service_type_id = t4.service_type_id and t.service_project_id = t4.service_project_id 


