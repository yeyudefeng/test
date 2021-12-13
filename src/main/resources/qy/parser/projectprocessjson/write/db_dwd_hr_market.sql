================================================================================
==========          clife_hr_customer_pension_tmp                     ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_pension_tmp stored as parquet as 
select 
 a.advisory_customer_id       as advisory_customer_id    --咨询客户id
,b.intention_customer_id      as intention_customer_id   --意向客户id
,b.old_man_id                 as old_man_id              --老人id
,if(b.customer_name is null,a.customer_name,b.customer_name)  as name   --客户姓名
,if(b.sex is null,a.sex,b.sex) as sex    --客户性别
,if(b.sales_consultant_id is null,a.sales_consultant_id,b.sales_consultant_id) as membership_adviser_id  --会籍顾问id
,if(b.sales_consultant_name is null,a.sales_consultant_name,b.sales_consultant_name) as membership_adviser_name  --会籍顾问姓名
,if(b.sub_org_id is null,a.sub_org_id,b.sub_org_id) sub_org_id   --机构id
,if(b.org_id is null,a.org_id,b.org_id) org_id   --集团id
,b.intentional_type  as intentional_type     --当前意向类型
,a.create_time       as adviser_time         --咨询时间
,b.assess_status    as assess_status       --评估状态
,b.create_time      as regist_time       -- 意向客户录入时间
from 
(select 
 advisory_customer_id    --咨询客户id
,customer_name           --客户姓名
,sex                     --客户性别
,sales_consultant_id     --会籍顾问id
,sales_consultant_name   --会籍顾问姓名
,sub_org_id              --机构id
,org_id                  --集团id
,create_time             --咨询时间
from db_ods_hr.clife_hr_ods_advisory_customer where part_date=regexp_replace(date_sub(current_date(),1),'-','') and status=1
) a 
full join 
(
select 
intention_customer_id           --意向客户id
,old_man_id                     --老人id
,customer_name                  --客户姓名
,sex                            --性别
,sales_consultant_id            --会籍顾问id
,sales_consultant_name          --会籍顾问姓名
,intentional_type               --当前意向类型
,linked_advisory_customer_id    --关联咨询客户id
,sub_org_id                     --机构id
,org_id                         --集团id
,assess_status           --评估状态
,create_time             -- 意向客户录入时间
from 
db_ods_hr.clife_hr_ods_intention_customer where part_date=regexp_replace(date_sub(current_date(),1),'-','') and status=1
) b 
on a.advisory_customer_id=b.linked_advisory_customer_id


================================================================================
==========          drop_clife_hr_customer_pension_tmp                ==========
================================================================================
drop table if exists  db_dw_hr.clife_hr_customer_pension_tmp


================================================================================
==========          clife_hr_customer_pension_his_tmp                 ==========
================================================================================
-- 2.初次意向、评估时间、发起入住通知时间从历史表拿
create table if not exists db_dw_hr.clife_hr_customer_pension_his_tmp stored as parquet as 
select
t.intention_customer_id 
,t.intentional_type   as first_intention_type
,t1.assess_time
,t2.ask_check_in_time
from 
(
select 
a1.intention_customer_id
,a1.intentional_type
from 
(
select 
intention_customer_id
,intentional_type
,row_number() over(partition by intention_customer_id order by op_time) as rank
from db_zip_campus.clife_hr_ods_intention_customer_his a 
where part_date<=regexp_replace(date_sub(current_date(),1),'-','')
) a1 where a1.rank=1) t
left join (
select 
intention_customer_id
,concat_ws(',',collect_list(op_time)) as assess_time
from db_zip_campus.clife_hr_ods_intention_customer_his a 
where part_date<=regexp_replace(date_sub(current_date(),1),'-','') and assess_status=1
group by a.intention_customer_id
) t1
on t.intention_customer_id=t1.intention_customer_id
left join 
(
select 
a1.intention_customer_id
,unix_timestamp(a1.op_time)   as ask_check_in_time
from 
(
select 
intention_customer_id
,op_time
,row_number() over(partition by intention_customer_id order by op_time) as rank
from db_zip_campus.clife_hr_ods_intention_customer_his a 
where part_date<=regexp_replace(date_sub(current_date(),1),'-','') and assess_status=4
) a1 where a1.rank=1
) t2
on t.intention_customer_id=t2.intention_customer_id


================================================================================
==========          drop_clife_hr_customer_pension_his_tmp            ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_pension_his_tmp


================================================================================
==========          clife_hr_customer_pension_assess_tmp              ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_pension_assess_tmp stored as parquet as 
select 
 old_man_id     --老人id
,concat_ws(',',collect_list(cast(assess_report_id as string))) as assess_report_ids 
,count(assess_report_id) as assess_cnt
from db_ods_hr.clife_hr_ods_assess_report where part_date=regexp_replace(date_sub(current_date(),1),'-','') and state=1
group by old_man_id


================================================================================
==========          drop_clife_hr_customer_pension_assess_tmp         ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_pension_assess_tmp


================================================================================
==========          clife_hr_customer_pension_remind_tmp              ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_pension_remind_tmp stored as parquet as 
select 
 a.customer_id
,a.return_visit_remind_time as remind_visit_time
from 
(select 
 customer_id     --客户id
,return_visit_remind_time    --回访提醒时间
,row_number() over(partition by customer_id order by return_visit_remind_time desc) as rank 
from db_ods_hr.clife_hr_ods_customer_visit_record where part_date=regexp_replace(date_sub(current_date(),1),'-','') and status=1) a
where rank=1 


================================================================================
==========          drop_clife_hr_customer_pension_remind_tmp         ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_pension_remind_tmp


================================================================================
==========          clife_hr_customer_pension_visit_tmp               ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_pension_visit_tmp stored as parquet as  
select 
 a.customer_id     --客户id
,concat_ws(',',collect_list(a.visit_date)) as visit_time
,count(a.visit_id)  as visit_cnt
from 
(
select 
customer_id
,from_unixtime(cast(visit_date as int),'yyyy-MM-dd HH:mm:ss')  as visit_date
,visit_id
from 
db_ods_hr.clife_hr_ods_customer_visit_record where part_date=regexp_replace(date_sub(current_date(),1),'-','') and status=1
) a 
group by a.customer_id


================================================================================
==========          drop_clife_hr_customer_pension_visit_tmp          ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_pension_visit_tmp


================================================================================
==========          clife_hr_customer_pension_info_tmp                ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_pension_info_tmp stored as parquet as
select
 a.advisory_customer_id  as advisory_customer_id               --  '咨询客户id'
,a.intention_customer_id  as intention_customer_id              --  '意向客户id'
,a.old_man_id  as old_man_id                         --  '老人id'
,a.name  as name                               --  '客户姓名'
,a.sex as sex                                --  '性别'
,cast(null as bigint) as room_id                            --  '房间id'
,cast(null as int) as pre_membership_id                  --  '预申请会籍id'
,cast(null as int) as membership_id                      --  '会籍id'
,c.assess_report_ids as assess_report_ids                  --  '评估报告id'
,cast(null as bigint)  as deposit_id                         --  '定金合同id'
,cast(null as bigint)  as sign_id                            --  '签约合同id'
,cast(null as string) as membership_owner_ids               --  '权益人id'
,cast(null as string) as transfor_to_ids                    --  '转让人id'
,cast(null as string) as transfor_from_ids                  --  '继承人id'
,cast(null as string) as trade_channel_ids                  --  '渠道id'
,cast(null as int) as product_id                         --  '会籍卡产品id'
,cast(null as int) as card_id                            --  '会籍卡id'
,cast(null as bigint) as nursing_homes_id                   --  '养老院id'
,cast(null as string) as nursing_homes_name                 --  '养老院名称'
,a.sub_org_id as sub_org_id                         --  '养老机构id'
,d.sub_org_name as sub_org_name                       --  '养老机构名称'
,a.org_id as org_id                             --  '集团id'
,d.org_name as org_name                           --  '集团名称'
,d.area_id as  area_id                            --  '区域id'
,a.membership_adviser_id as membership_adviser_id              --  '会籍顾问id'
,a.membership_adviser_name as membership_adviser_name            --  '会籍顾问姓名'
,a.adviser_time  as  adviser_time                       --  '咨询时间'
,a.regist_time as regist_time                       -- '意向客户录入时间'
,f.visit_time as visit_time                         --  '回访时间'
,b.assess_time as assess_time                        --  '评估时间'
,b.ask_check_in_time as ask_check_in_time                  --  '发起入住通知时间'
,cast(null as timestamp) as apply_time                         --  '申请签约时间'
,cast(null as timestamp) as deposit_time                       --  '下定时间'
,cast(null as timestamp) as sign_time                          --  '签约时间'
,cast(null as timestamp) as transfor_to_time                   --  '转让时间'
,cast(null as timestamp) as transfor_from_time                 --  '继承时间'
,cast(null as timestamp) as withdraw_time                      --  '退会时间'
,b.first_intention_type as first_intention_type               --  '首次意向'
,a.intentional_type as intention_type                     --  '当前意向'
,cast(null as int) as room_type                          --  '关注房型'
,a.assess_status as assess_status                      --  '评估状态'
,0 as customer_type                      --  '客户类型'
,cast(null as int) as pay_type_id                        --  '支付方式'
,f.visit_cnt  as visit_cnt                         --  '回访次数'
,e.remind_visit_time as remind_visit_time                  --  '回访提醒时间'
,c.assess_cnt as assess_cnt                         --  '评估次数'
,0 as deposit_amount                     --  '定金金额'
,0 as receivable_amount                  --  '应收金额'
,0 as received_amount                    --  '已收金额'
,0 as received_installment_amount        --  '已收分期金额'
,0 as received_occupancy_amount          --  '已收占用费'
,cast(null as int) as card_valid                         --  '卡有效期(年)'
,0 as membership_amount                  --  '会籍费'
,0 as overdue_amount                     --  '滞纳金'
,0 as received_overdue_amount            --  '已收滞纳金'
,0 as other_amount                       --  '其他费用'
,0 as manager_amount                     --  '管理费'
,cast(null as int) as living_limit                       --  '居住期限（月）'
,0 as trust_amount                       --  '托管费' 
,0 as membership_discount_amount         --  '会籍优惠价' 
,cast(null as int) as free_visit_day                     --  '免费探访天数' 
,0 as card_amount                        --  '卡费'
,0 as card_manager_amount                --  '卡管理费' 
,0 as other_transfor_to_amount           --  '其他转让费' 
,0 as transfor_to_amount                 --  '转让费' 
,cast(null as int) as transfor_to_type                   --  '转让类型' 
,0 as other_transfor_from_amount         --  '其他继承费' 
,0 as transfor_from_amount               --  '继承费'
,cast(null as int) as transfor_from_type                 --  '继承类型' 
,0 as remain_day                         --  '退会剩余天数' 
,0 as cancle_fee_status                  --  '是否退会费' 
,cast(null as int) as premium_type                       --  '退会类型' 
,0 as withdraw_amount                    --  '退会金额' 
,cast(null as bigint) as floor_id                            --  '楼层id'
,cast(null as bigint) as building_id                            --  '楼栋id'
,cast(null as int) as card_type                           --  '卡类型'
,cast(null as string) as card_name                           --  '卡名称'
,g.area_name as area_name                           --  '区域名称'
from 
db_dw_hr.clife_hr_customer_pension_tmp a 
left join 
db_dw_hr.clife_hr_customer_pension_his_tmp b 
on a.intention_customer_id=b.intention_customer_id 
left join 
db_dw_hr.clife_hr_customer_pension_assess_tmp c
on a.old_man_id=c.old_man_id
left join 	
db_dw_hr.clife_hr_dim_admin_sub_org d
on a.sub_org_id=d.sub_org_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-','') and d.status=1
left join 
db_dw_hr.clife_hr_customer_pension_remind_tmp e 
on a.intention_customer_id=e.customer_id
left join 
db_dw_hr.clife_hr_customer_pension_visit_tmp  f
on a.intention_customer_id=f.customer_id
left join db_dw_hr.clife_hr_dim_geographic_info  g
on d.area_id = g.area_id and g.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          drop_clife_hr_customer_pension_info_tmp           ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_pension_info_tmp


================================================================================
==========          clife_hr_customer_crm_tmp                         ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_tmp stored as parquet as 
select 
 a.customer_id as intention_customer_id    --意向客户id
,a.name        as name                     --客户姓名
,case when a.sex_option=40054 then 0 when a.sex_option=40055 then 1 else null end as sex   --性别
,a.intentional_type  as  intentional_type  --当前意向类型
,a.membership_adviser_id  as membership_adviser_id   --会籍顾问id
,b.worker_name  as membership_adviser_name     --会籍顾问名称
,c.trade_channel_ids  as trade_channel_ids     --渠道id
,a.sub_org_id     as  sub_org_id               --机构id
,d.sub_org_name   as  sub_org_name             --机构名称
,a.org_id         as  org_id                   --集团名称
,d.org_name       as  org_name                 --集团名称
,d.area_id
,d.area           as area_name
,a.create_time    as  regist_time              --意向客户录入时间
from db_ods_hr.clife_hr_ods_intentional_customer a
left join 
db_ods_hr.clife_hr_ods_worker_new b
on a.membership_adviser_id=b.nurse_worker_id and  b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
select 
 customer_id     --意向客户id
,concat_ws(',',collect_set(cast(channel_id as string))) as trade_channel_ids   --渠道ids
from 
db_ods_hr.clife_hr_ods_intentional_customer_channel
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by customer_id
) c 
on a.customer_id=c.customer_id
left join 
db_dw_hr.clife_hr_dim_admin_sub_org d
on a.sub_org_id=d.sub_org_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-','') and d.status=1
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and a.del_flag=0


================================================================================
==========          drop_clife_hr_customer_crm_tmp                    ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_tmp


================================================================================
==========          clife_hr_customer_crm_his_tmp                     ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_his_tmp stored as parquet as 
select 
a1.intention_customer_id
,a1.intentional_type  as first_intention_type
from 
(
select 
customer_id as intention_customer_id
,intentional_type
,row_number() over(partition by customer_id order by op_time) as rank
from db_zip_campus.clife_hr_ods_intentional_customer_his a 
where part_date<=regexp_replace(date_sub(current_date(),1),'-','')
) a1 where a1.rank=1


================================================================================
==========          drop_clife_hr_customer_crm_his_tmp                ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_his_tmp


================================================================================
==========          clife_hr_customer_crm_visit_tmp                   ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_visit_tmp stored as parquet as  
select 
 customer_id     --客户id
,concat_ws(',',collect_list(from_unixtime( cast(visit_date as int),'yyyy-MM-dd HH:mm:ss'))) as visit_time
,count(visit_id)  as visit_cnt
from db_ods_hr.clife_hr_ods_intentional_customer_visit where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by customer_id


================================================================================
==========          drop_clife_hr_customer_crm_visit_tmp              ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_visit_tmp


================================================================================
==========          clife_hr_customer_crm_remind_tmp                  ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_remind_tmp stored as parquet as 
select 
 a.customer_id
,a.return_visit_remind_time as remind_visit_time
from 
(select 
 customer_id     --客户id
,return_visit_remind_time    --回访提醒时间
,row_number() over(partition by customer_id order by return_visit_remind_time desc) as rank 
from db_ods_hr.clife_hr_ods_intentional_customer_visit where part_date=regexp_replace(date_sub(current_date(),1),'-','') ) a
where rank=1


================================================================================
==========          drop_clife_hr_customer_crm_remind_tmp             ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_remind_tmp


================================================================================
==========          clife_hr_customer_crm_deposit_sign_tmp            ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_deposit_sign_tmp stored as parquet as
select
     tmp.intention_customer_id              --  '意向客户id'
    ,tmp.room_id                            --  '房间id'
    ,tmp.membership_id                      --  '会籍id'
    ,tmp.deposit_id                         --  '定金合同id'
    ,tmp.sign_id                            --  '签约合同id'
    ,e.membership_owner_ids as membership_owner_ids    -- '权益人id'
    ,case when c.type =1 then c.pre_transfer_owners else null end as transfor_to_ids                    --  '转让人id'
    ,case when c.type =2 then c.pre_transfer_owners else null end as transfor_from_ids                  --  '继承人id'
    ,tmp.product_id                         --  '会籍卡产品id'
    ,f.card_id as card_id                            --  '会籍卡id'
    ,g.nursing_homes_id as nursing_homes_id                   --  '养老院id'
    ,j.nursing_homes_name as nursing_homes_name                 --  '养老院名称'
    ,j.sub_org_id as sub_org_id                         --  '养老机构id'
    ,j.sub_org_name as sub_org_name                       --  '养老机构名称'
    ,j.org_id as org_id                             --  '集团id'
    ,j.org_name as org_name                           --  '集团名称'
    ,j.area_id as  area_id                            --  '区域id'
    ,tmp.apply_id      --'预申请id'
    ,tmp.deposit_time                       --  '下定时间'
    ,tmp.sign_time                          --  '签约时间'
    ,case when c.type =1 then c.create_time else null end   as transfor_to_time                   --  '转让时间'
    ,case when c.type =2 then c.create_time else null end  as transfor_from_time                 --  '继承时间'
    ,unix_timestamp(d.create_time) as withdraw_time                      --  '退会时间'
    ,tmp.room_type                          --  '关注房型'  0单人间，1双人间，2小两房一厅 3一房一厅 4大两房一厅  5VIP单人间  6套间  7VIP套间   8两房一厅
    ,1 as customer_type                      --  '客户类型'
    ,tmp.deposit_amount                     --  '定金金额'
    ,tmp.card_valid                         --  '卡有效期(年)'
    ,tmp.membership_amount                  --  '会籍费'
    ,tmp.other_amount                       --  '其他费用'
    ,tmp.manager_amount                     --  '管理费'
    ,tmp.living_limit                       --  '居住期限（月）'
    ,tmp.trust_amount                       --  '托管费' 
    ,tmp.membership_discount_amount         --  '会籍优惠价' 
    ,tmp.free_visit_day                     --  '免费探访天数' 
    ,f.card_fee as card_amount                        --  '卡费'
    ,f.manager_fee as card_manager_amount                --  '卡管理费'
    ,case when c.type =1 then c.other_amount else null end  as other_transfor_to_amount           --  '其他转让费' 
    ,case when c.type =1 then c.transfer_fee else null end  as transfor_to_amount                 --  '转让费' 
    ,case when c.type =1 then c.transfer_nature else null end  as transfor_to_type                   --  '转让类型' 
    ,case when c.type =2 then c.other_amount else null end as other_transfor_from_amount         --  '其他继承费' 
    ,case when c.type =2 then c.transfer_fee else null end as transfor_from_amount               --  '继承费'
    ,case when c.type =2 then c.transfer_nature else null end as transfor_from_type                 --  '继承类型' 
    ,d.remain_day as remain_day                         --  '退会剩余天数' 
    ,d.cancle_fee_status as cancle_fee_status                  --  '是否退会费' 
    ,d.premium_type as premium_type                       --  '退会类型'  
    ,tmp.user_id   --'账户id'
    ,g.floor_id
    ,g.building_id
    ,h.type  as card_type
    ,h.card_name
    ,i.area_name
    ,tmp.contract_status
from (
    select 
         if(b.intent_user_id is null,a.intent_user_id,b.intent_user_id)  as intention_customer_id             
        ,if(b.room_id is null,a.room_id,b.room_id) as room_id                            
        ,if(b.membership_id is null,a.membership_id,b.membership_id) as membership_id 
        ,a.deposit_id  as deposit_id 
        ,b.sign_id
        ,b.product_id
        ,if(b.apply_id is null,a.apply_id,b.apply_id) as apply_id 
        ,a.create_time as deposit_time 
        ,b.create_time as sign_time       
        ,case when b.house_type=40066 then 0
              when b.house_type=40067 then 1
              when b.house_type=40068 then 2
              when b.house_type=40069 then 3
              when b.house_type=40095 then 4
              when b.house_type=40096 then 5
              when b.house_type=40097 then 6
              when b.house_type=40098 then 7
              when b.house_type=40277 then 8
              else null end as room_type  
        ,a.deposit_amount as deposit_amount 
        ,b.card_valid as card_valid                          
        ,b.membership_amount as membership_amount            
        ,b.other_amount as other_amount                      
        ,b.manager_fee as manager_amount                     
        ,b.living_limit as living_limit                      
        ,b.trust_fee as trust_amount                         
        ,b.membership_discount as membership_discount_amount 
        ,b.free_visit_day as free_visit_day   
        ,if(b.user_id is null,a.user_id,b.user_id)  as user_id   --'账户id'
        ,b.contract_status
    from (
        select 
             intent_user_id
            ,room_id
            ,membership_id
            ,deposit_id
            ,apply_id
            ,create_time
            ,deposit_amount
            ,deposit_num
            ,user_id
        from db_ods_hr.clife_hr_ods_contract_deposit t
        where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and t.deposit_status=1
    ) a 
    full join 
    (
        select 
             intent_user_id
            ,room_id
            ,membership_id
            ,sign_id
            ,product_id
            ,apply_id
            ,create_time
            ,house_type
            ,card_valid
            ,membership_amount
            ,other_amount
            ,manager_fee
            ,living_limit
            ,trust_fee
            ,membership_discount
            ,free_visit_day
            ,user_id
            ,contract_status
            ,deposit_num
            ,deposit_amount
        from db_ods_hr.clife_hr_ods_contract_sign t1
        where part_date=regexp_replace(date_sub(current_date(),1),'-','') and contract_status=1
    ) b
    on a.intent_user_id=b.intent_user_id and a.deposit_num=b.deposit_num and a.deposit_amount=b.deposit_amount
) tmp
left join 
(
select * from 
db_ods_hr.clife_hr_ods_membership_transfer
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and del_flag=0
) c 
on tmp.sign_id=c.pre_contract_id
left join 
(
select * from 
db_ods_hr.clife_hr_ods_membership_withdraw
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and del_flag=0
) d
on tmp.sign_id=d.contract_id
left join
(select contract_id,concat_ws(',',collect_set(cast(id as string))) as membership_owner_ids  from 
db_ods_hr.clife_hr_ods_membership_owner
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by contract_id
) e 
on tmp.sign_id=e.contract_id
left join 
(
select * from 
db_ods_hr.clife_hr_ods_card_product  
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and del_flag=0
) f
on tmp.product_id=f.product_id
left join 
(select * from 
db_dw_hr.clife_hr_dim_location_room  
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
) g on 
tmp.room_id=g.room_id
left join 
(select * from 
db_dw_hr.clife_hr_dim_card  
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and del_flag=0) h
on f.card_id=h.card_id
left join 
db_dw_hr.clife_hr_dim_nursing_home  j
on g.nursing_homes_id=j.nursing_homes_id and j.status=1 and j.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info i
on j.area_id=i.area_id and i.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_customer_crm_apply_tmp                   ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_apply_tmp stored as parquet as
select 
a.intention_customer_id              --  '意向客户id'
,a.room_id                            --  '房间id'
,b.membership_id as pre_membership_id                  --  '预申请会籍id'
,a.membership_id                      --  '会籍id'
,a.deposit_id                         --  '定金合同id'
,a.sign_id                            --  '签约合同id'
,a.membership_owner_ids               --  '权益人id'
,a.transfor_to_ids                    --  '转让人id'
,a.transfor_from_ids                  --  '继承人id'
,a.product_id                         --  '会籍卡产品id'
,a.card_id                            --  '会籍卡id'
,a.nursing_homes_id                   --  '养老院id'
,a.nursing_homes_name                 --  '养老院名称'
,a.sub_org_id                         --  '养老机构id'
,a.sub_org_name                       --  '养老机构名称'
,a.org_id                             --  '集团id'
,a.org_name                           --  '集团名称'
,a.area_id                            --  '区域id'
,b.create_time as apply_time                         --  '申请签约时间'
,a.deposit_time                       --  '下定时间'
,a.sign_time                          --  '签约时间'
,a.transfor_to_time                   --  '转让时间'
,a.transfor_from_time                 --  '继承时间'
,a.withdraw_time                      --  '退会时间'
,a.room_type                          --  '关注房型'
,a.customer_type                      --  '客户类型'
,a.deposit_amount                     --  '定金金额'
,a.card_valid                         --  '卡有效期(年)'
,a.membership_amount                  --  '会籍费'
,a.other_amount                       --  '其他费用'
,a.manager_amount                     --  '管理费'
,a.living_limit                       --  '居住期限（月）'
,a.trust_amount                       --  '托管费' 
,a.membership_discount_amount         --  '会籍优惠价' 
,a.free_visit_day                     --  '免费探访天数'
,a.card_amount                        --  '卡费'
,a.card_manager_amount                --  '卡管理费' 
,a.other_transfor_to_amount           --  '其他转让费' 
,a.transfor_to_amount                 --  '转让费' 
,a.transfor_to_type                   --  '转让类型' 
,a.other_transfor_from_amount         --  '其他继承费' 
,a.transfor_from_amount               --  '继承费'
,a.transfor_from_type                 --  '继承类型' 
,a.remain_day                         --  '退会剩余天数' 
,a.cancle_fee_status                  --  '是否退会费' 
,a.premium_type                       --  '退会类型' 
,a.user_id                            --  '账户id'
,a.floor_id
,a.building_id
,a.card_type
,a.card_name
,a.area_name
,a.contract_status
from  
db_dw_hr.clife_hr_customer_crm_deposit_sign_tmp a 
left join 
db_ods_hr.clife_hr_ods_membership_apply b 
on a.apply_id=b.apply_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') and b.del_flag=0


================================================================================
==========          drop_clife_hr_customer_crm_apply_tmp              ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_apply_tmp


================================================================================
==========          clife_hr_customer_crm_fee_detail_tmp              ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_fee_detail_tmp stored as parquet as
select 
 t.sign_id   -- 合同编号
,sum(t.receivable_amount) as receivable_amount  --  '应收金额'
,sum(t.received_amount) as  received_amount  --  '已收金额'
,sum(t.received_installment_amount) as received_installment_amount    --  '已收分期金额'
,sum(t.received_occupancy_amount) as received_occupancy_amount  --  '已收占用费'
,sum(t.overdue_amount)  as overdue_amount   --'滞纳金'
,sum(t.received_overdue_amount) as received_overdue_amount    --  '已收滞纳金'
,sum(t.withdraw_amount) as withdraw_amount  --  '退会金额' 
from 
(
select 
 relation_id as sign_id   -- 合同编号
,b.confirm_amount as receivable_amount  --  '应收金额'
,case when b.status=1 then b.confirm_amount else 0 end as received_amount  --  '已收金额'
,b.stage_sum  as received_installment_amount    --  '已收分期金额'
,b.occupation_sum  as received_occupancy_amount  --  '已收占用费'
,b.overdue_fine as overdue_amount      --'滞纳金'
,b.overdue_sum as received_overdue_amount    --  '已收滞纳金'
,case when a.type=4 then b.confirm_amount else 0 end as withdraw_amount  --  '退会金额' 
from 
db_ods_hr.clife_hr_ods_fee a 
left join 
db_ods_hr.clife_hr_ods_fee_detail b
on a.fee_id=b.fee_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and a.del_flag=0
) t
group by t.sign_id


================================================================================
==========          drop_clife_hr_customer_crm_fee_detail_tmp         ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_fee_detail_tmp


================================================================================
==========          clife_hr_customer_crm_fee_type_tmp                ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_fee_type_tmp stored as parquet as
select 
 t.sign_id   -- 合同编号
,max(pay_type_id) as pay_type_id  --支付方式  0刷卡，1现金，2微信，3支付宝，4其他
from 
(
select 
 a.relation_id as sign_id   -- 合同编号
,case when a.receive_type_option=40090 then 0 
      when a.receive_type_option=40091 then 1 
	  when a.receive_type_option=40092 then 2 
	  when a.receive_type_option=40093 then 3 
	  when a.receive_type_option=40094 then 4
      else null end pay_type_id	  
from 
db_ods_hr.clife_hr_ods_fee a 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and a.del_flag=0
) t
group by t.sign_id


================================================================================
==========          drop_clife_hr_customer_crm_fee_type_tmp           ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_fee_type_tmp


================================================================================
==========          clife_hr_customer_crm_sign_fee_detail_tmp         ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_sign_fee_detail_tmp stored as parquet as
select 
a.intention_customer_id              --  '意向客户id'
,a.room_id                            --  '房间id'
,a.pre_membership_id                  --  '预申请会籍id'
,a.membership_id                      --  '会籍id'
,a.deposit_id                         --  '定金合同id'
,a.sign_id                            --  '签约合同id'
,a.membership_owner_ids               --  '权益人id'
,a.transfor_to_ids                    --  '转让人id'
,a.transfor_from_ids                  --  '继承人id'
,a.product_id                         --  '会籍卡产品id'
,a.card_id                            --  '会籍卡id'
,a.nursing_homes_id                   --  '养老院id'
,a.nursing_homes_name                 --  '养老院名称'
,a.sub_org_id                         --  '养老机构id'
,a.sub_org_name                       --  '养老机构名称'
,a.org_id                             --  '集团id'
,a.org_name                           --  '集团名称'
,a.area_id                            --  '区域id'
,a.apply_time                         --  '申请签约时间'
,a.deposit_time                       --  '下定时间'
,a.sign_time                          --  '签约时间'
,a.transfor_to_time                   --  '转让时间'
,a.transfor_from_time                 --  '继承时间'
,a.withdraw_time                      --  '退会时间'
,a.room_type                          --  '关注房型'
,a.customer_type                      --  '客户类型'
,a.deposit_amount                     --  '定金金额'
,b.receivable_amount  --  '应收金额'
,b.received_amount  --  '已收金额'
,b.received_installment_amount    --  '已收分期金额'
,b.received_occupancy_amount  --  '已收占用费'
,a.card_valid                         --  '卡有效期(年)'
,a.membership_amount                  --  '会籍费'
,b.overdue_amount   --'滞纳金'
,b.received_overdue_amount    --  '已收滞纳金'
,a.other_amount                       --  '其他费用'
,a.manager_amount                     --  '管理费'
,a.living_limit                       --  '居住期限（月）'
,a.trust_amount                       --  '托管费' 
,a.membership_discount_amount         --  '会籍优惠价' 
,a.free_visit_day                     --  '免费探访天数'
,a.card_amount                        --  '卡费'
,a.card_manager_amount                --  '卡管理费' 
,a.other_transfor_to_amount           --  '其他转让费' 
,a.transfor_to_amount                 --  '转让费' 
,a.transfor_to_type                   --  '转让类型' 
,a.other_transfor_from_amount         --  '其他继承费' 
,a.transfor_from_amount               --  '继承费'
,a.transfor_from_type                 --  '继承类型' 
,a.remain_day                         --  '退会剩余天数' 
,a.cancle_fee_status                  --  '是否退会费' 
,a.premium_type                       --  '退会类型' 
,b.withdraw_amount  --  '退会金额' 
,a.user_id                            --  '账户id'
,a.floor_id
,a.building_id
,a.card_type
,a.card_name
,a.area_name
,c.pay_type_id                        --  '支付方式id'
,a.contract_status
from 
db_dw_hr.clife_hr_customer_crm_apply_tmp a 
left join 
db_dw_hr.clife_hr_customer_crm_fee_detail_tmp b 
on a.sign_id=b.sign_id
left join 
db_dw_hr.clife_hr_customer_crm_fee_type_tmp c 
on a.sign_id=c.sign_id


================================================================================
==========          drop_clife_hr_customer_crm_sign_fee_detail_tmp    ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_sign_fee_detail_tmp


================================================================================
==========          clife_hr_customer_crm_old_man_tmp                 ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_old_man_tmp stored as parquet as
select
a.*,
c.old_man_id      -- '老人id'
,d.assess_report_ids   --'评估报告id'
,d.assess_cnt          -- '评估次数'
from 
db_dw_hr.clife_hr_customer_crm_sign_fee_detail_tmp  a 
left join db_ods_hr.clife_hr_ods_account_account  b 
on a.user_id = b.account_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') and b.status=1
left join db_ods_hr.clife_hr_ods_old_man_extend  c 
on b.phone=c.linkman_phone and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_customer_pension_assess_tmp d 
on c.old_man_id=d.old_man_id


================================================================================
==========          drop_clife_hr_customer_crm_old_man_tmp            ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_old_man_tmp


================================================================================
==========          clife_hr_customer_crm_base_info_tmp               ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_base_info_tmp stored as parquet as
select 
 a.intention_customer_id --意向客户id
,a.name        as name                     --客户姓名
,a.sex   --性别
,a.intentional_type  as  intentional_type  --当前意向类型
,a.membership_adviser_id  as membership_adviser_id   --会籍顾问id
,a.membership_adviser_name     --会籍顾问名称
,a.trade_channel_ids     --渠道id
,b.first_intention_type    -- 首次意向
,a.regist_time             --意向客户录入时间
,c.visit_time              --回访时间
,c.visit_cnt               --回访次数
,d.remind_visit_time       --提醒回访时间
,a.sub_org_id               --机构id
,a.sub_org_name             --机构名称
,a.org_id                   --集团名称
,a.org_name                 --集团名称
,a.area_id
,a.area_name
from
db_dw_hr.clife_hr_customer_crm_tmp a 
left join 
db_dw_hr.clife_hr_customer_crm_his_tmp b 
on a.intention_customer_id=b.intention_customer_id
left join 
db_dw_hr.clife_hr_customer_crm_visit_tmp c 
on a.intention_customer_id=c.customer_id
left join 
db_dw_hr.clife_hr_customer_crm_remind_tmp  d 
on a.intention_customer_id=d.customer_id


================================================================================
==========          drop_clife_hr_customer_crm_base_info_tmp          ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_base_info_tmp


================================================================================
==========          clife_hr_customer_crm_info_tmp                    ==========
================================================================================
create table if not exists db_dw_hr.clife_hr_customer_crm_info_tmp stored as parquet as
select 
a.*
,b.room_id                            --  '房间id'
,b.pre_membership_id                  --  '预申请会籍id'
,b.membership_id                      --  '会籍id'
,b.deposit_id                         --  '定金合同id'
,b.sign_id                            --  '签约合同id'
,b.membership_owner_ids               --  '权益人id'
,b.transfor_to_ids                    --  '转让人id'
,b.transfor_from_ids                  --  '继承人id'
,b.product_id                         --  '会籍卡产品id'
,b.card_id                            --  '会籍卡id'
,b.nursing_homes_id                   --  '养老院id'
,b.nursing_homes_name                 --  '养老院名称'
,b.apply_time                         --  '申请签约时间'
,b.deposit_time                       --  '下定时间'
,b.sign_time                          --  '签约时间'
,b.transfor_to_time                   --  '转让时间'
,b.transfor_from_time                 --  '继承时间'
,b.withdraw_time                      --  '退会时间'
,b.room_type                          --  '关注房型'
,b.customer_type                      --  '客户类型'
,b.deposit_amount                     --  '定金金额'
,b.receivable_amount  --  '应收金额'
,b.received_amount  --  '已收金额'
,b.received_installment_amount    --  '已收分期金额'
,b.received_occupancy_amount  --  '已收占用费'
,b.card_valid                         --  '卡有效期(年)'
,b.membership_amount                  --  '会籍费'
,b.overdue_amount   --'滞纳金'
,b.received_overdue_amount    --  '已收滞纳金'
,b.other_amount                       --  '其他费用'
,b.manager_amount                     --  '管理费'
,b.living_limit                       --  '居住期限（月）'
,b.trust_amount                       --  '托管费' 
,b.membership_discount_amount         --  '会籍优惠价' 
,b.free_visit_day                     --  '免费探访天数'
,b.card_amount                        --  '卡费'
,b.card_manager_amount                --  '卡管理费' 
,b.other_transfor_to_amount           --  '其他转让费' 
,b.transfor_to_amount                 --  '转让费' 
,b.transfor_to_type                   --  '转让类型' 
,b.other_transfor_from_amount         --  '其他继承费' 
,b.transfor_from_amount               --  '继承费'
,b.transfor_from_type                 --  '继承类型' 
,b.remain_day                         --  '退会剩余天数' 
,b.cancle_fee_status                  --  '是否退会费' 
,b.withdraw_amount  --  '退会金额' 
,b.premium_type
,b.floor_id
,b.building_id
,b.card_type
,b.card_name
,b.pay_type_id                        --  '支付方式id'
,b.old_man_id
,b.assess_report_ids   --'评估报告id'
,b.assess_cnt          -- '评估次数'
,b.contract_status
from db_dw_hr.clife_hr_customer_crm_base_info_tmp  a 
left join 
db_dw_hr.clife_hr_customer_crm_old_man_tmp b
on a.intention_customer_id=b.intention_customer_id


================================================================================
==========          drop_clife_hr_customer_crm_info_tmp               ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_info_tmp


================================================================================
==========          clife_hr_dwd_market                               ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dwd_market partition(part_date) 
select
advisory_customer_id               --  '咨询客户id'
,intention_customer_id              --  '意向客户id'
,old_man_id                         --  '老人id'
,name                               --  '客户姓名'
,sex                                --  '性别'
,room_id                            --  '房间id'
,pre_membership_id                  --  '预申请会籍id'
,membership_id                      --  '会籍id'
,assess_report_ids                  --  '评估报告id'
,deposit_id                         --  '定金合同id'
,sign_id                            --  '签约合同id'
,membership_owner_ids               --  '权益人id'
,transfor_to_ids                    --  '转让人id'
,transfor_from_ids                  --  '继承人id'
,trade_channel_ids                  --  '渠道id'
,product_id                         --  '会籍卡产品id'
,card_id                            --  '会籍卡id'
,nursing_homes_id                   --  '养老院id'
,nursing_homes_name                 --  '养老院名称'
,sub_org_id                         --  '养老机构id'
,sub_org_name                       --  '养老机构名称'
,org_id                             --  '集团id'
,org_name                           --  '集团名称'
,area_id                            --  '区域id'
,membership_adviser_id              --  '会籍顾问id'
,membership_adviser_name            --  '会籍顾问姓名'
,adviser_time                       --  '咨询时间'
,visit_time                         --  '回访时间'
,regist_time                        --  '意向客户录入时间'
,assess_time                        --  '评估时间'
,cast(ask_check_in_time as timestamp) as  ask_check_in_time                   --  '发起入住通知时间'
,apply_time                         --  '申请签约时间'
,deposit_time                       --  '下定时间'
,sign_time                          --  '签约时间'
,transfor_to_time                   --  '转让时间'
,transfor_from_time                 --  '继承时间'
,withdraw_time                      --  '退会时间'
,first_intention_type               --  '首次意向'
,intention_type                     --  '当前意向'
,room_type                          --  '关注房型'
,assess_status                      --  '评估状态'
,customer_type                      --  '客户类型'
,pay_type_id                        --  '支付方式'
,visit_cnt                          --  '回访次数'
,remind_visit_time                  --  '回访提醒时间'
,assess_cnt                         --  '评估次数'
,deposit_amount                     --  '定金金额'
,receivable_amount                  --  '应收金额'
,received_amount                    --  '已收金额'
,received_installment_amount        --  '已收分期金额'
,received_occupancy_amount          --  '已收占用费'
,card_valid                         --  '卡有效期(年)'
,membership_amount                  --  '会籍费'
,overdue_amount                     --  '滞纳金'
,received_overdue_amount            --  '已收滞纳金'
,other_amount                       --  '其他费用'
,manager_amount                     --  '管理费'
,living_limit                       --  '居住期限（月）'
,trust_amount                       --  '托管费' 
,membership_discount_amount         --  '会籍优惠价' 
,free_visit_day                     --  '免费探访天数' 
,card_amount                        --  '卡费'
,card_manager_amount                --  '卡管理费' 
,other_transfor_to_amount           --  '其他转让费' 
,transfor_to_amount                 --  '转让费' 
,transfor_to_type                   --  '转让类型' 
,other_transfor_from_amount         --  '其他继承费' 
,transfor_from_amount               --  '继承费'
,transfor_from_type                 --  '继承类型' 
,remain_day                         --  '退会剩余天数' 
,cancle_fee_status                  --  '是否退会费' 
,premium_type                       --  '退会类型' 
,withdraw_amount                    --  '退会金额' 
,floor_id
,building_id
,card_type
,card_name
,area_name
,cast(null as int) as contract_status
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_dw_hr.clife_hr_customer_pension_info_tmp 
union all 
select
 cast(null as bigint) as advisory_customer_id               --  '咨询客户id'
,intention_customer_id              --  '意向客户id'
,old_man_id                         --  '老人id'
,name                               --  '客户姓名'
,sex                                --  '性别'
,room_id                            --  '房间id'
,pre_membership_id                  --  '预申请会籍id'
,membership_id                      --  '会籍id'
,assess_report_ids                  --  '评估报告id'
,deposit_id                         --  '定金合同id'
,sign_id                            --  '签约合同id'
,membership_owner_ids               --  '权益人id'
,transfor_to_ids                    --  '转让人id'
,transfor_from_ids                  --  '继承人id'
,trade_channel_ids                  --  '渠道id'
,product_id                         --  '会籍卡产品id'
,card_id                            --  '会籍卡id'
,nursing_homes_id                   --  '养老院id'
,nursing_homes_name                 --  '养老院名称'
,sub_org_id                         --  '养老机构id'
,sub_org_name                       --  '养老机构名称'
,org_id                             --  '集团id'
,org_name                           --  '集团名称'
,area_id                            --  '区域id'
,membership_adviser_id              --  '会籍顾问id'
,membership_adviser_name            --  '会籍顾问姓名'
,cast(null as timestamp) as adviser_time                       --  '咨询时间'
,visit_time                         --  '回访时间'
,regist_time                        --  '意向客户录入时间'
,cast(null as string) as assess_time                        --  '评估时间'
,cast(null as timestamp) as ask_check_in_time                  --  '发起入住通知时间'
,apply_time                         --  '申请签约时间'
,deposit_time                       --  '下定时间'
,sign_time                          --  '签约时间'
,transfor_to_time                   --  '转让时间'
,transfor_from_time                 --  '继承时间'
,cast(withdraw_time as timestamp) as  withdraw_time                        --  '退会时间'
,first_intention_type               --  '首次意向'
,intentional_type as intention_type                     --  '当前意向'
,room_type                          --  '关注房型'
,cast(null as int) asassess_status                      --  '评估状态'
,customer_type                      --  '客户类型'
,pay_type_id                        --  '支付方式'
,visit_cnt                          --  '回访次数'
,remind_visit_time                  --  '回访提醒时间'
,assess_cnt                         --  '评估次数'
,deposit_amount                     --  '定金金额'
,receivable_amount                  --  '应收金额'
,received_amount                    --  '已收金额'
,received_installment_amount        --  '已收分期金额'
,received_occupancy_amount          --  '已收占用费'
,card_valid                         --  '卡有效期(年)'
,membership_amount                  --  '会籍费'
,overdue_amount                     --  '滞纳金'
,received_overdue_amount            --  '已收滞纳金'
,other_amount                       --  '其他费用'
,manager_amount                     --  '管理费'
,living_limit                       --  '居住期限（月）'
,trust_amount                       --  '托管费' 
,membership_discount_amount         --  '会籍优惠价' 
,free_visit_day                     --  '免费探访天数' 
,card_amount                        --  '卡费'
,card_manager_amount                --  '卡管理费' 
,other_transfor_to_amount           --  '其他转让费' 
,transfor_to_amount                 --  '转让费' 
,transfor_to_type                   --  '转让类型' 
,other_transfor_from_amount         --  '其他继承费' 
,transfor_from_amount               --  '继承费'
,transfor_from_type                 --  '继承类型' 
,remain_day                         --  '退会剩余天数' 
,cancle_fee_status                  --  '是否退会费' 
,premium_type                       --  '退会类型' 
,withdraw_amount                    --  '退会金额' 
,floor_id
,building_id
,card_type
,card_name
,area_name
,contract_status
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from db_dw_hr.clife_hr_customer_crm_info_tmp


================================================================================
==========          drop_clife_hr_customer_crm_deposit_sign_tmp       ==========
================================================================================
drop table if exists db_dw_hr.clife_hr_customer_crm_deposit_sign_tmp


