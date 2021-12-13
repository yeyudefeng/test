create table if not exists db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp stored as parquet as
select
-- 营销管理模块 对intention_customer_id分组，取出一些数据，对部分数据做集合。
 a.intention_customer_id --意向客户id
,max(a.name)        as name                     --客户姓名
,max(a.sex)     sex --性别
,max(a.intention_type)  as  intentional_type  --当前意向类型
,max(a.membership_adviser_id)  as membership_adviser_id   --会籍顾问id
,max(a.membership_adviser_name)  membership_adviser_name   --会籍顾问名称
,max(a.trade_channel_ids)   trade_channel_ids   --渠道id
,max(a.sub_org_id) sub_org_id
,max(a.first_intention_type) first_intention_type   -- 首次意向
,max(a.regist_time)     regist_time        --意向客户录入时间
,max(a.visit_time)      visit_time        --回访时间
,max(a.visit_cnt)       visit_cnt        --回访次数
,max(a.remind_visit_time)   remind_visit_time    --提醒回访时间
,max(a.assess_time)   assess_time         -- 评估时间
,concat_ws(',',collect_list(from_unixtime( cast(a.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time_list      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(a.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time_list       -- '签约时间集合'
from db_dw_hr.clife_hr_dwd_market a 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by a.intention_customer_id;



/**
营销机构维度
db_dw_hr.clife_hr_dwd_market
db_dw_hr.clife_hr_dwm_market
针对dwd，dwm数据基于sub_org_id做分组，计算此粒度的各种汇总数据（客户数，金额，次数，房间数等）。
不对日期做分组
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_sub_org_f partition(part_date)
select
if(b1.sub_org_id is null,a1.sub_org_id,b1.sub_org_id) as sub_org_id                          --   '养老机构id',
,if(b1.sub_org_name is null,a1.sub_org_name,b1.sub_org_name) as sub_org_name                        --   '养老机构名称'
,if(b1.org_id is null,a1.org_id,b1.org_id) as org_id                              --   '养老集团id'
,if(b1.org_id is null,a1.org_id,b1.org_id) as org_name                            --   '养老集团名称'
,if(b1.area_id is null,a1.area_id,b1.area_id) as area_id                             --   '区域id'
,if(b1.area_name is null,a1.area_name,b1.area_name) as area_name                           --   '区域名称'
,a1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a1.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a1.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a1.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a1.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a1.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a1.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a1.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,c1.transfer_to_member_cnt              --   '转让会籍数'
,c1.transfer_from_member_cnt            --   '继承会籍数'
,c1.withdraw_member_cnt                 --   '退会会籍数'
,a1.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a1.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,b1.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,b1.deposit_cust_visit_cnt              --   '下定客户回访次数'
,b1.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a1.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a1.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,b1.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,b1.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,b1.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,e1.card_pay_amount                     --	  '刷卡金额'
,e1.cash_pay_amount                     --   '现金金额'
,e1.weixin_pay_amount                   --   '微信支付金额'
,e1.alipay_amount                       --   '阿里支付金额'	
,e1.other_pay_amount                    --   '其他方式支付金额'
,e1.received_amount                     --   '已收金额'
,e1.received_installment_amount         --   '已收分期金额'
,e1.received_occupancy_amount           --   '已收占用费'
,e1.membership_amount                   --   '会籍费'
,e1.overdue_amount                      --   '滞纳金'
,e1.received_overdue_amount             --   '已收滞纳金'
,e1.other_amount                        --   '其他费用'
,e1.manager_amount                      --   '管理费'
,e1.trust_amount                        --   '托管费' 
,e1.membership_discount_amount          --   '会籍优惠价' 
,e1.card_amount                         --   '卡费'
,e1.card_manager_amount                 --   '卡管理费'
,e1.other_transfor_to_amount            --   '其他转让费' 
,e1.transfor_to_amount                  --   '转让费'
,e1.other_transfor_from_amount          --   '其他继承费' 
,e1.transfor_from_amount                --   '继承费'
,e1.withdraw_amount                     --   '退会金额' 
,(a1.assess_cust_cnt+b1.assess_cust_cnt)  as assess_cust_cnt                     --   '评估客户数'
,a1.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,(a1.assess_cnt+b1.assess_cnt)  as assess_cnt                          --   '评估总次数'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(select
a.sub_org_id
,max(a.sub_org_name) sub_org_name
,max(a.org_id)    org_id
,max(org_name)    org_name
,max(area_id)     area_id
,max(area_name)   area_name
,count(distinct advisory_customer_id)  as advisory_customer_cnt     --'咨询客户数'
,sum(first_high_cnt)  as pension_first_intent_high_cnt  --'客户跟踪模块首次意向感兴趣客户数'
,sum(first_mid_cnt)  as pension_first_intent_mid_cnt  --'客户跟踪模块首次意向兴趣一般客户数'
,sum(first_low_cnt)  as pension_first_intent_low_cnt  --'客户跟踪模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as pension_first_intent_highest_cnt  --'客户跟踪模块首次意向希望购买客户数'
,sum(high_cnt)     as pension_intent_high_cnt  --'客户跟踪模块意向感兴趣客户数'
,sum(mid_cnt)      as pension_intent_mid_cnt   --'客户跟踪模块意向兴趣一般客户数'
,sum(low_cnt)      as pension_intent_low_cnt   --'客户跟踪模块意向不感兴趣客户数'
,sum(highest_cnt)  as pension_intent_highest_cnt  --'客户跟踪模块意向希望购买客户数'
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
,sum(advisory_visit_cnt)  as advisory_cust_visit_cnt   --'咨询客户回访次数'
,sum(intention_visit_cnt)   as pension_intent_cust_visit_cnt  -- '客户跟踪模块意向客户回访次数'
,sum(advisory_visit_cnt)/count(advisory_customer_id)  as avg_advisory_cust_visit_cnt  --'咨询客户平均回访次数'
,sum(intention_visit_cnt)/count(distinct intention_customer_id)  as avg_pension_intent_cust_visit_cnt  --'客户跟踪模块意向客户平均回访次数'
,sum(assess_cnt)  as assess_cnt  --'总评估次数'
,count(distinct assess_cust_cnt) as assess_cust_cnt
,count(distinct ask_check_in_cust) as ask_check_in_cust_cnt
from 
(select 
 t.advisory_customer_id
,t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,case when t.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when t.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when t.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when t.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when t.intention_type=1 then 1 else 0 end as high_cnt
,case when t.intention_type=2 then 1 else 0 end as mid_cnt
,case when t.intention_type=3 then 1 else 0 end as low_cnt
,case when t.intention_type=4 then 1 else 0 end as highest_cnt
,case when t.advisory_customer_id is not null then visit_cnt else 0 end advisory_visit_cnt
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
,t.assess_cnt                       -- '评估次数'
,case when t.assess_cnt>0 then t.intention_customer_id else null end assess_cust_cnt
,case when ask_check_in_time is not null then t.intention_customer_id else null end ask_check_in_cust
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0) a
group by a.sub_org_id)  a1
full join
(
 select
 t1.sub_org_id
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(t1.visit_cnt)   as crm_intent_cust_visit_cnt  -- '营销管理模块意向客户回访次数'
,sum(t1.visit_cnt)/count(distinct intention_customer_id)  as avg_crm_intent_cust_visit_cnt  --'营销管理模块意向客户平均回访次数'
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
,sum(deposit_cust_visit_cnt)   as deposit_cust_visit_cnt  --'下定客户回访总次数'
,sum(sign_cust_vist_cnt)   as   sign_cust_visit_cnt    -- '签约客户回访总次数'
,sum(deposit_cust_visit_cnt)/sum(deposit_cust_cnt)  as avg_deposit_cust_visit_cnt  --'下定客户平均回访次数'
,sum(sign_cust_vist_cnt)/sum(sign_cust_cnt)  as avg_sign_cust_visit_cnt  --'签约客户平均回访次数'
,sum(assess_cnt) assess_cnt
,sum(assess_cust_cnt)  assess_cust_cnt
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt
from 
(
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt                       
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id) b1 
on a1.sub_org_id=b1.sub_org_id
left join 
(
select
t1.sub_org_id
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select 
c.membership_id
,c.sub_org_id
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select 
 t.membership_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id)  c
) t1
group by t1.sub_org_id) c1
on b1.sub_org_id=c1.sub_org_id
left join 
(
select
t1.sub_org_id
,sum(t1.deposit_room_cnt)  deposit_room_cnt
,sum(t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,case when deposit_time is null then 1 else 0 end as deposit_room_cnt
,case when sign_time   is null then 1 else 0 end as sign_room_cnt
from 
(select 
 t.room_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.room_id)  c
) t1
group by t1.sub_org_id) d1
on b1.sub_org_id=d1.sub_org_id
left join 
(select 
 sub_org_id
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '应收金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
,sum(other_transfor_from_amount)  other_transfor_from_amount    --  '其他继承费'
,sum(transfor_from_amount)        transfor_from_amount       --  '继承费'
,sum(withdraw_amount)             withdraw_amount        --  '退会金额'
from db_dw_hr.clife_hr_dwm_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by sub_org_id
) e1
on b1.sub_org_id=e1.sub_org_id;




------------------------------ 养老机构营销日统计表

/**
营销机构维度，日期维度（天）
按照sub_org_id，以及各种行为日期（天）分组，full join，统计当天各种行为的多种汇总数据
 */

create table if not exists db_dw_hr.clife_hr_dws_market_by_sub_org_d_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(ta1.sub_org_id is null,'',ta1.sub_org_id),',',if(ta2.sub_org_id is null,'',ta2.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a1.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a1.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a1.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a1.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a1.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a1.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a1.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfer_to_amount            --   '其他转让费' 
,tt8.transfer_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,(ta1.assess_cust_cnt+tt6.assess_cust_cnt)  as assess_cust_cnt                     --   '评估客户数'
,ta2.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,(ta1.assess_cnt+tt6.assess_cnt)  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(a1.regist_date is null,'',a1.regist_date),',',if(tt1.adviser_date is null,'',tt1.adviser_date),',',if(ta1.assess_date is null,'',ta1.assess_date),',',if(ta2.ask_date is null,'',ta2.ask_date),',',if(tt4.visit_date is null,'',tt4.visit_date),',',if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt5.visit_date is null,'',tt5.visit_date),',',if(tt6.assess_date is null,'',tt6.assess_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',','')),',')[1]  as data_date                  -- 数据日期
from 
(select
-- 客户跟踪模块：对sub_org_id，regist_date分组，sum，计算不同意向客户数。
a.sub_org_id
,a.regist_date
,sum(a.first_high_cnt)  as pension_first_intent_high_cnt  --'客户跟踪模块首次意向感兴趣客户数'
,sum(a.first_mid_cnt)  as pension_first_intent_mid_cnt  --'客户跟踪模块首次意向兴趣一般客户数'
,sum(a.first_low_cnt)  as pension_first_intent_low_cnt  --'客户跟踪模块首次意向不感兴趣客户数'
,sum(a.first_highest_cnt)  as pension_first_intent_highest_cnt  --'客户跟踪模块首次意向希望购买客户数'
,sum(a.high_cnt)     as pension_intent_high_cnt  --'客户跟踪模块意向感兴趣客户数'
,sum(a.mid_cnt)      as pension_intent_mid_cnt   --'客户跟踪模块意向兴趣一般客户数'
,sum(a.low_cnt)      as pension_intent_low_cnt   --'客户跟踪模块意向不感兴趣客户数'
,sum(a.highest_cnt)  as pension_intent_highest_cnt  --'客户跟踪模块意向希望购买客户数'
,count(distinct a.intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select
-- 客户跟踪模块 客户过滤，转换意向客户类型数据为数量。转换regist_time为日期，计算不同意向客户数量
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy-MM-dd') as regist_date                        -- '意向客户录入日期'
,case when t.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when t.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when t.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when t.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when t.intention_type=1 then 1 else 0 end as high_cnt
,case when t.intention_type=2 then 1 else 0 end as mid_cnt
,case when t.intention_type=3 then 1 else 0 end as low_cnt
,case when t.intention_type=4 then 1 else 0 end as highest_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) as a
group by a.sub_org_id,a.regist_date)  a1
full join 
(select
-- 客户跟踪模块 的客户数据按照sub_org_id，adviser_date分组，对咨询客户count distinct。
a.sub_org_id
,a.adviser_date
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select
-- 客户跟踪模块 的数据，转换adviser_time为日期，计算回访次数
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.adviser_time as int),'yyyy-MM-dd') as adviser_date                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.adviser_date)  tt1
on a1.sub_org_id=tt1.sub_org_id  and a1.regist_date=tt1.adviser_date
full join
(
select
-- 客户跟踪模块 按sub_org_id，assess_date（评估日期）分组，计算 评估次数 评估客户数
a.sub_org_id
,a.assess_date
,count(distinct a.intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(a.assess_time2)           as   assess_cnt              --'评估次数'
from 
(
select
-- 客户跟踪模块 炸开回访时间集合
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,10) as assess_date                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(assess_time,','))  l1 as assess_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0) as a 
group by a.sub_org_id,a.assess_date) as  ta1
on a1.sub_org_id=ta1.sub_org_id  and a1.regist_date=ta1.assess_date
full join 
(
select
-- 客户跟踪模块 按sub_org_id，ask_date（发起入住通知日期）分组，计算发起入住客户通知数
-- 问题：为什么这里不加distinct
aa.sub_org_id
,aa.ask_date
,count(aa.intention_customer_id)   ask_check_in_cust_cnt            --'发起入住通知客户数'
from 
(
-- 客户跟踪模块 转换ask_check_in_time（发起入住通知时间）
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.ask_check_in_time as int),'yyyy-MM-dd') as ask_date                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) aa
group by aa.sub_org_id,aa.ask_date
)  ta2
on a1.sub_org_id=ta2.sub_org_id and a1.regist_date=ta2.ask_date
full join
(
select
-- 客户跟踪模块 对sub_org_id，visit_date分组，计算咨询客户回访次数，意向客户回访次数，咨询客户人均回访次数，意向客户人均回访次数
aa4.sub_org_id
,aa4.visit_date
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select
-- 客户跟踪模块 炸开回访时间，转换意向客户回访次数，咨询客户回访次数
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,10) as visit_date                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.visit_date
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.regist_date=tt4.visit_date
full join 
(
select
-- 营销管理模块 对sub_org_id，regist_date（意向客户录入日期）做分组，计算不同意向客户数
 t1.sub_org_id
,t1.regist_date
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select
-- 营销管理模块 转换意向类型为具体类型，转为数量
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
from 
(
select
-- 营销管理模块 对intention_customer_id分组，计算首次意向，当前意向，评估次数，意向客户录入时间。
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt                       
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.regist_date) b1 
on a1.sub_org_id=b1.sub_org_id and a1.regist_date=b1.regist_date
full join 
(
select
-- 营销管理模块 对sub_org_id，deposit_date（下定日期）分组，计算下定客户数，下定额，下定会籍数，下定房间数。
aa2.sub_org_id
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
-- 营销管理模块 转换下定时间
 t.intention_customer_id
,sub_org_id 
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.deposit_date
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.regist_date=tt2.deposit_date
full join 
(
select
-- 营销管理模块 对sub_org_id，sign_date（签约日期）分组，计算签约客户数（去重），汇总各种金额，计算签约会籍数量，签约房间数量。
aa3.sub_org_id
,aa3.sign_date
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
-- 营销管理模块 转换不同支付方式，取出各种金额，转换sign_time（签约时间）
 t.intention_customer_id
,sub_org_id 
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.sign_date
)  tt3
on a1.sub_org_id=tt3.sub_org_id and a1.regist_date=tt3.sign_date
full join 
(
select
-- 营销管理模块 按sub_org_id，visit_date分组，计算下定客户回访次数，下定客户人数，下定客户平均回访次数等，签约等。。
aa5.sub_org_id
,aa5.visit_date
,count(distinct aa5.intention_customer_id)  int_cnt
,count(aa5.visit_time2)  as crm_intent_cust_visit_cnt
,count(aa5.visit_time2)/count(distinct aa5.intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(aa5.deposit_visit_cnt)  as deposit_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)/sum(aa5.deposit_visit_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(aa5.sign_visit_cnt)  as sign_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)/sum(aa5.sign_visit_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select
-- 营销管理模块 从clife_hr_customer_crm_visit_base_info_tmp表，炸开visit_time，转换回访日期，转换下定回访次数，签约回访次数，下定意向客户id，签约意向客户id
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,10) as visit_date                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
) as aa5 
group by aa5.sub_org_id,aa5.visit_date
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.regist_date=tt5.visit_date
full join 
(
select
-- 营销管理模块 对sub_org_id，assess_date分组，计算发起评估的客户数，评估次数
aa6.sub_org_id
,aa6.assess_date
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select
-- 营销管理模块 炸开评估时间，转换评估日期
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,10) as assess_date                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
)  aa6 
group by aa6.sub_org_id,aa6.assess_date
)  tt6
on a1.sub_org_id=tt6.sub_org_id and a1.regist_date=tt6.assess_date
full join 
(
select
-- 营销管理模块 对sub_org_id，apply_date（会籍申请日期）分组计算预申请会籍数量
aa7.sub_org_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select
-- 营销管理模块 转换会籍申请时间
t.pre_membership_id
,t.sub_org_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.apply_date
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.regist_date=tt7.apply_date
full join 
(
select
-- 营销管理模块 对sub_org_id，transfer_to_date（日期）分组，汇总转让费，计算转让会籍数量
aa8.sub_org_id
,aa8.transfer_to_date
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfer_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfer_to_amount      --  '转让费'
from
(
select
-- 营销管理模块 计算转让时间，转让费
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd')  transfer_to_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.transfer_to_date
) tt8
on a1.sub_org_id=tt8.sub_org_id and a1.regist_date=tt8.transfer_to_date
full join 
(
select
-- 营销管理模块 按sub_org_id，transfer_from_date分组，计算继承会籍数量，汇总继承会籍费用
aa9.sub_org_id
,aa9.transfer_from_date
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfor_from_amount)    other_transfer_from_amount     
,sum(transfor_from_amount)          transfer_from_amount  
from   
(
select
-- 营销管理模块 转换继承时间，继承费用
t.membership_id
,t.other_transfor_from_amount       
,t.transfor_from_amount            
,t.sub_org_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd')  transfer_from_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id,aa9.transfer_from_date
) tt9
on a1.sub_org_id=tt9.sub_org_id and a1.regist_date=tt9.transfer_from_date
full join 
(
select
-- 营销管理模块 按sub_org_id，withdraw_date分组，计算退会会籍数量，汇总退会金额
aa10.sub_org_id
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount 
from    
(
select
-- 营销管理模块 转换退会日期
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd')  withdraw_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.withdraw_date
) tt10
on a1.sub_org_id=tt10.sub_org_id and a1.regist_date=tt10.withdraw_date;



/**
营销机构维度，日期维度（天）
db_dw_hr.clife_hr_dws_market_by_sub_org_d_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，地理维表，加入维度数据。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_sub_org_d partition(part_date)
select
a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfer_to_amount            --  '其他转让费' 
,a.transfer_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_sub_org_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 








/**
营销机构维度，日期维度（月）
逻辑与上上表（clife_hr_dws_market_by_sub_org_d_tmp）相似，只是日期维度从天转为月
计算客户跟踪模块，营销管理模块。每个sub_org_id，每月的各种数据计算
 */

create table if not exists db_dw_hr.clife_hr_dws_market_by_sub_org_m_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(ta1.sub_org_id is null,'',ta1.sub_org_id),',',if(ta2.sub_org_id is null,'',ta2.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a1.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a1.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a1.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a1.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a1.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a1.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a1.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfer_to_amount            --   '其他转让费' 
,tt8.transfer_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,(ta1.assess_cust_cnt+tt6.assess_cust_cnt)  as assess_cust_cnt                     --   '评估客户数'
,ta2.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,(ta1.assess_cnt+tt6.assess_cnt)  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(a1.regist_month is null,'',a1.regist_month),',',if(tt1.adviser_month is null,'',tt1.adviser_month),',',if(ta1.assess_month is null,'',ta1.assess_month),',',if(ta2.ask_month is null,'',ta2.ask_month),',',if(tt4.visit_month is null,'',tt4.visit_month),',',if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt5.visit_month is null,'',tt5.visit_month),',',if(tt6.assess_month is null,'',tt6.assess_month),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_month is null,'',tt8.transfer_to_month),',',if(tt9.transfer_from_month is null,'',tt9.transfer_from_month),',',if(tt10.withdraw_month is null,'',tt10.withdraw_month),',','')),',')[1]  as data_month                  -- 数据日期
from 
(
select
a.sub_org_id
,a.regist_month
,sum(first_high_cnt)  as pension_first_intent_high_cnt  --'客户跟踪模块首次意向感兴趣客户数'
,sum(first_mid_cnt)  as pension_first_intent_mid_cnt  --'客户跟踪模块首次意向兴趣一般客户数'
,sum(first_low_cnt)  as pension_first_intent_low_cnt  --'客户跟踪模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as pension_first_intent_highest_cnt  --'客户跟踪模块首次意向希望购买客户数'
,sum(high_cnt)     as pension_intent_high_cnt  --'客户跟踪模块意向感兴趣客户数'
,sum(mid_cnt)      as pension_intent_mid_cnt   --'客户跟踪模块意向兴趣一般客户数'
,sum(low_cnt)      as pension_intent_low_cnt   --'客户跟踪模块意向不感兴趣客户数'
,sum(highest_cnt)  as pension_intent_highest_cnt  --'客户跟踪模块意向希望购买客户数'
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy-MM') as regist_month                        -- '意向客户录入日期'
,case when t.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when t.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when t.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when t.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when t.intention_type=1 then 1 else 0 end as high_cnt
,case when t.intention_type=2 then 1 else 0 end as mid_cnt
,case when t.intention_type=3 then 1 else 0 end as low_cnt
,case when t.intention_type=4 then 1 else 0 end as highest_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,regist_month
)  a1
full join 
(select
a.sub_org_id
,a.adviser_month
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.adviser_time as int),'yyyy-MM') as adviser_month                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.adviser_month)  tt1
on a1.sub_org_id=tt1.sub_org_id  and a1.regist_month=tt1.adviser_month
full join
(
select 
a.sub_org_id
,a.assess_month
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,7) as assess_month                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  a 
group by a.sub_org_id,a.assess_month
)  ta1
on a1.sub_org_id=ta1.sub_org_id  and a1.regist_month=ta1.assess_month
full join 
(
select
aa.sub_org_id
,aa.ask_month
,count(aa.intention_customer_id)   ask_check_in_cust_cnt            --'发起入住通知客户数'
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.ask_check_in_time as int),'yyyy-MM') as ask_month                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) aa
group by aa.sub_org_id,aa.ask_month
)  ta2
on a1.sub_org_id=ta2.sub_org_id and a1.regist_month=ta2.ask_month
full join
(
select
aa4.sub_org_id
,aa4.visit_month
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,7) as visit_month                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.visit_month
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.regist_month=tt4.visit_month
full join 
(
 select
 t1.sub_org_id
,t1.regist_month
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM')  as regist_month
from 
(
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt                       
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.regist_month) b1 
on a1.sub_org_id=b1.sub_org_id and a1.regist_month=b1.regist_month
full join 
(
select 
aa2.sub_org_id
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.deposit_month
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.sign_month
)  tt3
on a1.sub_org_id=tt3.sub_org_id and a1.regist_month=tt3.sign_month
full join 
(
select
aa5.sub_org_id
,aa5.visit_month
,count(distinct aa5.intention_customer_id)  int_cnt
,count(aa5.visit_time2)  as crm_intent_cust_visit_cnt
,count(aa5.visit_time2)/count(distinct aa5.intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(aa5.deposit_visit_cnt)  as deposit_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)/sum(aa5.deposit_visit_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(aa5.sign_visit_cnt)  as sign_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)/sum(aa5.sign_visit_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,7) as visit_month                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
) as aa5 
group by aa5.sub_org_id,aa5.visit_month
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.regist_month=tt5.visit_month
full join 
(
select 
aa6.sub_org_id
,aa6.assess_month
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,7) as assess_month                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
)  aa6 
group by aa6.sub_org_id,aa6.assess_month
)  tt6
on a1.sub_org_id=tt6.sub_org_id and a1.regist_month=tt6.assess_month
full join 
(
select
aa7.sub_org_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.apply_date
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.regist_month=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.transfer_to_month
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfer_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfer_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM')  transfer_to_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.transfer_to_month
) tt8
on a1.sub_org_id=tt8.sub_org_id and a1.regist_month=tt8.transfer_to_month
full join 
(
select
aa9.sub_org_id
,aa9.transfer_from_month
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfor_from_amount)    other_transfer_from_amount     
,sum(transfor_from_amount)          transfer_from_amount  
from   
(
select 
t.membership_id
,t.other_transfor_from_amount       
,t.transfor_from_amount            
,t.sub_org_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM')  transfer_from_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id,aa9.transfer_from_month
) tt9
on a1.sub_org_id=tt9.sub_org_id and a1.regist_month=tt9.transfer_from_month
full join 
(
select
aa10.sub_org_id
,aa10.withdraw_month
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount 
from    
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM')  withdraw_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.withdraw_month
) tt10
on a1.sub_org_id=tt10.sub_org_id and a1.regist_month=tt10.withdraw_month;





/**
营销机构维度，日期维度（月）
逻辑与上上表（db_dw_hr.clife_hr_dws_market_by_sub_org_d）相似，只是日期维度从天转为月
计算客户跟踪模块，营销管理模块。每个sub_org_id，每月的各种数据计算

db_dw_hr.clife_hr_dws_market_by_sub_org_m_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，地理维表，加入维度数据。
 */

insert overwrite table db_dw_hr.clife_hr_dws_market_by_sub_org_m partition(part_date)
select
a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfer_to_amount            --  '其他转让费' 
,a.transfer_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_sub_org_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  



-------------------------------------------------
/**
营销机构维度，日期维度（年）
逻辑与上上上上表（clife_hr_dws_market_by_sub_org_d_tmp）相似，只是日期维度从天转为年
计算客户跟踪模块，营销管理模块。每个sub_org_id，每年的各种数据计算
 */

create table if not exists db_dw_hr.clife_hr_dws_market_by_sub_org_y_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(ta1.sub_org_id is null,'',ta1.sub_org_id),',',if(ta2.sub_org_id is null,'',ta2.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a1.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a1.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a1.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a1.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a1.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a1.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a1.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfer_to_amount            --   '其他转让费' 
,tt8.transfer_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,(ta1.assess_cust_cnt+tt6.assess_cust_cnt)  as assess_cust_cnt                     --   '评估客户数'
,ta2.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,(ta1.assess_cnt+tt6.assess_cnt)  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(a1.regist_year is null,'',a1.regist_year),',',if(tt1.adviser_year is null,'',tt1.adviser_year),',',if(ta1.assess_year is null,'',ta1.assess_year),',',if(ta2.ask_year is null,'',ta2.ask_year),',',if(tt4.visit_year is null,'',tt4.visit_year),',',if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt5.visit_year is null,'',tt5.visit_year),',',if(tt6.assess_year is null,'',tt6.assess_year),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_year is null,'',tt8.transfer_to_year),',',if(tt9.transfer_from_year is null,'',tt9.transfer_from_year),',',if(tt10.withdraw_year is null,'',tt10.withdraw_year),',','')),',')[1]  as data_year                  -- 数据日期
from 
(
select
a.sub_org_id
,a.regist_year
,sum(first_high_cnt)  as pension_first_intent_high_cnt  --'客户跟踪模块首次意向感兴趣客户数'
,sum(first_mid_cnt)  as pension_first_intent_mid_cnt  --'客户跟踪模块首次意向兴趣一般客户数'
,sum(first_low_cnt)  as pension_first_intent_low_cnt  --'客户跟踪模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as pension_first_intent_highest_cnt  --'客户跟踪模块首次意向希望购买客户数'
,sum(high_cnt)     as pension_intent_high_cnt  --'客户跟踪模块意向感兴趣客户数'
,sum(mid_cnt)      as pension_intent_mid_cnt   --'客户跟踪模块意向兴趣一般客户数'
,sum(low_cnt)      as pension_intent_low_cnt   --'客户跟踪模块意向不感兴趣客户数'
,sum(highest_cnt)  as pension_intent_highest_cnt  --'客户跟踪模块意向希望购买客户数'
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy') as regist_year                        -- '意向客户录入日期'
,case when t.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when t.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when t.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when t.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when t.intention_type=1 then 1 else 0 end as high_cnt
,case when t.intention_type=2 then 1 else 0 end as mid_cnt
,case when t.intention_type=3 then 1 else 0 end as low_cnt
,case when t.intention_type=4 then 1 else 0 end as highest_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,regist_year
)  a1
full join 
(select
a.sub_org_id
,a.adviser_year
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.adviser_time as int),'yyyy') as adviser_year                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.adviser_year)  tt1
on a1.sub_org_id=tt1.sub_org_id  and a1.regist_year=tt1.adviser_year
full join
(
select 
a.sub_org_id
,a.assess_year
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,4) as assess_year                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  a 
group by a.sub_org_id,a.assess_year
)  ta1
on a1.sub_org_id=ta1.sub_org_id  and a1.regist_year=ta1.assess_year
full join 
(
select
aa.sub_org_id
,aa.ask_year
,count(aa.intention_customer_id)   ask_check_in_cust_cnt            --'发起入住通知客户数'
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,from_unixtime( cast(t.ask_check_in_time as int),'yyyy') as ask_year                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) aa
group by aa.sub_org_id,aa.ask_year
)  ta2
on a1.sub_org_id=ta2.sub_org_id and a1.regist_year=ta2.ask_year
full join
(
select
aa4.sub_org_id
,aa4.visit_year
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,4) as visit_year                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.visit_year
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.regist_year=tt4.visit_year
full join 
(
 select
 t1.sub_org_id
,t1.regist_year
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy')  as regist_year
from 
(
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt                       
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.regist_year) b1 
on a1.sub_org_id=b1.sub_org_id and a1.regist_year=b1.regist_year
full join 
(
select 
aa2.sub_org_id
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.deposit_year
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.sign_year
)  tt3
on a1.sub_org_id=tt3.sub_org_id and a1.regist_year=tt3.sign_year
full join 
(
select
aa5.sub_org_id
,aa5.visit_year
,count(distinct aa5.intention_customer_id)  int_cnt
,count(aa5.visit_time2)  as crm_intent_cust_visit_cnt
,count(aa5.visit_time2)/count(distinct aa5.intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(aa5.deposit_visit_cnt)  as deposit_visit_cnt
,count(distinct aa5.deposit_visit_cust_cnt)/sum(aa5.deposit_visit_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(aa5.sign_visit_cnt)  as sign_visit_cnt
,count(distinct aa5.sign_visit_cust_cnt)/sum(aa5.sign_visit_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,visit_time2
,substr(visit_time2,1,4) as visit_year                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
) as aa5 
group by aa5.sub_org_id,aa5.visit_year
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.regist_year=tt5.visit_year
full join 
(
select 
aa6.sub_org_id
,aa6.assess_year
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,4) as assess_year                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
)  aa6 
group by aa6.sub_org_id,aa6.assess_year
)  tt6
on a1.sub_org_id=tt6.sub_org_id and a1.regist_year=tt6.assess_year
full join 
(
select
aa7.sub_org_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.apply_date
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.regist_year=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.transfer_to_year
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfer_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfer_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy')  transfer_to_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.transfer_to_year
) tt8
on a1.sub_org_id=tt8.sub_org_id and a1.regist_year=tt8.transfer_to_year
full join 
(
select
aa9.sub_org_id
,aa9.transfer_from_year
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfor_from_amount)    other_transfer_from_amount     
,sum(transfor_from_amount)          transfer_from_amount  
from   
(
select 
t.membership_id
,t.other_transfor_from_amount       
,t.transfor_from_amount            
,t.sub_org_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy')  transfer_from_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id,aa9.transfer_from_year
) tt9
on a1.sub_org_id=tt9.sub_org_id and a1.regist_year=tt9.transfer_from_year
full join 
(
select
aa10.sub_org_id
,aa10.withdraw_year
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount 
from    
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy')  withdraw_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.withdraw_year
) tt10
on a1.sub_org_id=tt10.sub_org_id and a1.regist_year=tt10.withdraw_year;






/**
营销机构维度，日期维度（年）
逻辑与上上上上表（db_dw_hr.clife_hr_dws_market_by_sub_org_d）相似，只是日期维度从天转为年
计算客户跟踪模块，营销管理模块。每个sub_org_id，每年的各种数据计算

db_dw_hr.clife_hr_dws_market_by_sub_org_y_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，地理维表，加入维度数据。
 */

insert overwrite table db_dw_hr.clife_hr_dws_market_by_sub_org_y partition(part_date)
select
a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_first_intent_high_cnt       --   '客户跟踪模块首次意向感兴趣客户数'
,a.pension_first_intent_mid_cnt        --   '客户跟踪模块首次意向兴趣一般客户数'
,a.pension_first_intent_low_cnt        --   '客户跟踪模块首次意向不感兴趣客户数'
,a.pension_first_intent_highest_cnt    --   '客户跟踪模块首次意向希望购买客户数'
,a.pension_intent_high_cnt             --   '客户跟踪模块当前意向感兴趣客户数'
,a.pension_intent_mid_cnt              --   '客户跟踪模块当前意向兴趣一般客户数'
,a.pension_intent_low_cnt              --   '客户跟踪模块当前意向不感兴趣客户数'
,a.pension_intent_highest_cnt          --   '客户跟踪模块当前意向希望购买客户数'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfer_to_amount            --  '其他转让费' 
,a.transfer_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.ask_check_in_cust_cnt               --   '发起入住通知客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_sub_org_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  


-----------------------------------渠道累计
/**
营销机构维度，渠道维度
应该跟前面的一样，之前是根据sub_org_id累计，这里是根据渠道累计。
客户跟踪模块 不涉及 渠道信息，这里不计算。
仅 营销管理模块 包含 渠道信息。

db_dw_hr.clife_hr_dwd_market
db_dw_hr.clife_hr_dim_trade_channel (渠道维表)

按sub_org_id，channel_id分组，计算各种金额，人数，会籍，房间等汇总
全量计算。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_channel_f partition(part_date)
select 
a.channel_id                          --  '渠道id'
,a.sub_org_id                          --  '养老机构id'
,max(a.sub_org_name)  sub_org_name                      --  '养老机构名称'
,max(a.org_id)    org_id                          --  '养老集团id'
,max(a.org_name)   org_name                         --  '养老集团名称'
,max(a.area_id)    area_id                         --  '区域id'
,max(a.area_name)   area_name                        --  '区域名称'
,max(b.trade_channel_name) channel_name                        --  '渠道名称'
,max(b.channel_type)    channel_type                     --  '渠道类型'
,max(b.channel_type_name)   channel_type_name                --  '渠道类型名称'
,count( distinct intention_customer_id) crm_intent_cnt                      --  '营销管理模块意向客户数'
,count( distinct deposit_cust) deposit_cust_cnt                    --  '下定客户数'
,count( distinct deposit_cust)/count( distinct intention_customer_id)  as deposit_ratio                       --  '下定转化率'
,count( distinct sign_cust) sign_cust_cnt                       --  '签约客户数'
,count( distinct sign_cust)/count( distinct deposit_cust) as sign_ratio                          --  '签约转化率'
,count(distinct pre_membership_id)  apply_member_cnt                    --  '预申请会籍数量'
,count(distinct deposit_member)  deposit_member_cnt                  --  '下定会籍数量'
,count(distinct deposit_room) deposit_room_cnt                    --  '下定房间数量'
,count(distinct sign_member) sign_member_cnt                     --  '签约会籍数量'
,count(distinct sign_room) sign_room_cnt                       --  '签约房间数量'
,sum(deposit_amount) deposit_amount                      -- '下定额'
,sum(receivable_amount) sales_amount                        -- '销售额'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(
select
-- 营销管理模块 炸开渠道id，获取客户，会籍，房间，金额等信息
intention_customer_id              --  '意向客户id'
,pre_membership_id                  --  '预申请会籍id'
,membership_id                      --  '会籍id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,sub_org_name                       --  '养老机构名称'
,org_id                             --  '集团id'
,org_name                           --  '集团名称'
,area_id                            --  '区域id'
,area_name
,deposit_time                       --  '下定时间'
,sign_time                          --  '签约时间'
,deposit_amount                     --  '定金金额'
,receivable_amount                  --  '应收金额'
,channel_id
,case when deposit_time is not null then intention_customer_id else null end as deposit_cust
,case when sign_time is not null then intention_customer_id else null end as sign_cust
,case when deposit_time is not null then membership_id else null end as deposit_member
,case when sign_time is not null then membership_id else null end as sign_member
,case when deposit_time is not null then room_id else null end as deposit_room
,case when sign_time is not null then room_id else null end as sign_room
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  a 
left join 
db_dw_hr.clife_hr_dim_trade_channel  b
on a.channel_id=b.id and  b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by a.sub_org_id,a.channel_id





----------------------------------渠道日表

/**
营销机构维度，渠道维度，日期维度（天）

营销管理模块
按sub_org_id，channel_id，date分组，计算各种客户数，会籍数，房间数，金额等
 */

create table db_dw_hr.clife_hr_dws_market_by_channel_d_tmp stored as parquet as 
select
split(getSet2(concat(if(tt1.channel_id is null,'',tt1.channel_id),',',if(tt2.channel_id is null,'',tt2.channel_id),',',if(tt3.channel_id is null,'',tt3.channel_id),',',if(tt4.channel_id is null,'',tt4.channel_id),',','')),',')[1]  as channel_id                          --  '渠道id'
,split(getSet2(concat(if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',','')),',')[1]  as sub_org_id                           --  '养老机构id'
,tt4.crm_intent_cnt                      --  '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --  '下定客户数'
,tt3.sign_cust_cnt                       --  '签约客户数'
,tt1.apply_member_cnt                    --  '预申请会籍数量'
,tt2.deposit_member_cnt                  --  '下定会籍数量'
,tt2.deposit_room_cnt                    --  '下定房间数量'
,tt3.sign_member_cnt                     --  '签约会籍数量'
,tt3.sign_room_cnt                       --  '签约房间数量'
,tt2.deposit_amount                      -- '下定额'
,tt3.sales_amount                        -- '销售额'
,split(getSet2(concat(if(tt1.apply_date is null,'',tt1.apply_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt4.regist_date is null,'',tt4.regist_date),',','')),',')[1]  as data_date
from 
(select
-- 按sub_org_id，channel_id，apply_date分组，计算预申请会籍数量
a.sub_org_id
,a.channel_id
,a.apply_date  
,count(distinct pre_membership_id)     as apply_member_cnt 
from 
(
select
-- 营销管理模块 炸开渠道id，取出会籍id，申请时间
pre_membership_id                      --  '会籍id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,apply_time                       --  '申请时间'
,channel_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  as apply_date    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.apply_date
) tt1
full join 
(select
-- 营销管理模块 按sub_org_id，channel_id，deposit_date分组，计算下定人数，下定房间数，下定金额，下定会籍数。
a.sub_org_id
,a.channel_id
,a.deposit_date
,count(distinct intention_customer_id)  as deposit_cust_cnt   
,count(distinct membership_id)     as deposit_member_cnt
,count(distinct room_id)     as deposit_room_cnt
,sum(deposit_amount)         as deposit_amount   
from 
(
select
-- 营销管理模块 炸开渠道id，取出下定时间，会籍id，下定金额
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id                            --  '房间id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,deposit_time                       --  '下定时间'
,deposit_amount                     --  '定金金额'
,channel_id
,from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd')  as deposit_date    -- '下定日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.deposit_date
) tt2
on tt1.sub_org_id=tt2.sub_org_id and tt1.channel_id=tt2.channel_id and tt1.apply_date=tt2.deposit_date
full join 
(select
-- 营销管理模块 按sub_org_id，channel_id，sign_date分组，计算签约客户数，签约会籍数，签约房间数，销售额等
a.sub_org_id
,a.channel_id
,a.sign_date
,count(distinct intention_customer_id)  as sign_cust_cnt   
,count(distinct membership_id)     as sign_member_cnt
,count(distinct room_id)     as sign_room_cnt
,sum(receivable_amount)         as sales_amount   
from 
(
select
-- 营销管理模块 炸开渠道id，取出签约日期，金额等
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,sign_time                       --  '签约时间'
,receivable_amount                     --  '应收金额'
,channel_id
,from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')  as sign_date    -- '签约日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.sign_date
) tt3
on tt1.sub_org_id=tt3.sub_org_id and tt1.channel_id=tt3.channel_id and tt1.apply_date=tt3.sign_date
full join 
(select
-- 营销管理模块 按sub_org_id，channel_id，regist_date分组，计算crm录入意向客户数量
a.sub_org_id
,a.channel_id
,a.regist_date  
,count(distinct intention_customer_id)     as crm_intent_cnt 
from 
(
select
-- 营销管理模块 炸开渠道id，取出意向客户的录入时间
intention_customer_id              --  '意向客户id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,regist_time                       --  '录入时间'
,channel_id
,from_unixtime( cast(t.regist_time as int),'yyyy-MM-dd')  as regist_date    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.regist_date
) tt4
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_date=tt4.regist_date;


/**
营销机构维度，渠道维度，日期维度（天）
db_dw_hr.clife_hr_dws_market_by_channel_d_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_trade_channel
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，渠道维表，地理维表，加入维度数据。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_channel_d partition(part_date)
select 
a.channel_id                         --   '渠道id'
,a.sub_org_id                         --   '养老机构id'
,b.sub_org_name                       --   '养老机构名称'
,b.org_id                             --   '养老集团id'
,b.org_name                           --   '养老集团名称'
,d.area_id                            --   '区域id'
,d.area as area_name                          --   '区域名称'
,c.trade_channel_name                       --   '渠道名称'
,c.channel_type                        --   '渠道类型'
,c.channel_type_name                  --   '渠道类型名称'
,a.crm_intent_cnt                     --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                   --   '下定客户数'
,a.sign_cust_cnt                      --   '签约客户数'
,a.apply_member_cnt                   --   '预申请会籍数量'
,a.deposit_member_cnt                 --   '下定会籍数量'
,a.deposit_room_cnt                   --   '下定房间数量'
,a.sign_member_cnt                    --   '签约会籍数量'
,a.sign_room_cnt                      --   '签约房间数量'
,a.deposit_amount                     --  '下定额'
,a.sales_amount                       --  '销售额'
,a.data_date
,regexp_replace(date_sub(current_date(),1),'-','')  part_date
from 
db_dw_hr.clife_hr_dws_market_by_channel_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_trade_channel c 
on a.channel_id=c.id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 



--------------------------渠道  月
/**
营销机构维度，渠道维度，日期维度（月）

营销管理模块
按sub_org_id，channel_id，month分组，计算各种客户数，会籍数，房间数，金额等
 */
create table db_dw_hr.clife_hr_dws_market_by_channel_m_tmp stored as parquet as 
select
split(getSet2(concat(if(tt1.channel_id is null,'',tt1.channel_id),',',if(tt2.channel_id is null,'',tt2.channel_id),',',if(tt3.channel_id is null,'',tt3.channel_id),',',if(tt4.channel_id is null,'',tt4.channel_id),',','')),',')[1]  as channel_id                          --  '渠道id'
,split(getSet2(concat(if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',','')),',')[1]  as sub_org_id                           --  '养老机构id'
,tt4.crm_intent_cnt                      --  '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --  '下定客户数'
,tt3.sign_cust_cnt                       --  '签约客户数'
,tt1.apply_member_cnt                    --  '预申请会籍数量'
,tt2.deposit_member_cnt                  --  '下定会籍数量'
,tt2.deposit_room_cnt                    --  '下定房间数量'
,tt3.sign_member_cnt                     --  '签约会籍数量'
,tt3.sign_room_cnt                       --  '签约房间数量'
,tt2.deposit_amount                      -- '下定额'
,tt3.sales_amount                        -- '销售额'
,split(getSet2(concat(if(tt1.apply_month is null,'',tt1.apply_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt4.regist_month is null,'',tt4.regist_month),',','')),',')[1]  as data_month
from 
(select 
a.sub_org_id
,a.channel_id
,a.apply_month  
,count(distinct pre_membership_id)     as apply_member_cnt 
from 
(
select 
pre_membership_id                      --  '会籍id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,apply_time                       --  '申请时间'
,channel_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  as apply_month    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.apply_month
) tt1
full join 
(select 
a.sub_org_id
,a.channel_id
,a.deposit_month
,count(distinct intention_customer_id)  as deposit_cust_cnt   
,count(distinct membership_id)     as deposit_member_cnt
,count(distinct room_id)     as deposit_room_cnt
,sum(deposit_amount)         as deposit_amount   
from 
(
select 
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,deposit_time                       --  '下定时间'
,deposit_amount                     --  '定金金额'
,channel_id
,from_unixtime( cast(t.deposit_time as int),'yyyy-MM')  as deposit_month    -- '下定日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.deposit_month
) tt2
on tt1.sub_org_id=tt2.sub_org_id and tt1.channel_id=tt2.channel_id and tt1.apply_month=tt2.deposit_month
full join 
(select 
a.sub_org_id
,a.channel_id
,a.sign_month
,count(distinct intention_customer_id)  as sign_cust_cnt   
,count(distinct membership_id)     as sign_member_cnt
,count(distinct room_id)     as sign_room_cnt
,sum(receivable_amount)         as sales_amount   
from 
(
select 
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,sign_time                       --  '签约时间'
,receivable_amount                     --  '应收金额'
,channel_id
,from_unixtime( cast(t.sign_time as int),'yyyy-MM')  as sign_month    -- '签约日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.sign_month
) tt3
on tt1.sub_org_id=tt3.sub_org_id and tt1.channel_id=tt3.channel_id and tt1.apply_month=tt3.sign_month
full join 
(select 
a.sub_org_id
,a.channel_id
,a.regist_month  
,count(distinct intention_customer_id)     as crm_intent_cnt 
from 
(
select 
intention_customer_id              --  '意向客户id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,regist_time                       --  '录入时间'
,channel_id
,from_unixtime( cast(t.regist_time as int),'yyyy-MM')  as regist_month    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.regist_month
) tt4
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_month=tt4.regist_month;


/**
营销机构维度，渠道维度，日期维度（月）
db_dw_hr.clife_hr_dws_market_by_channel_m_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_trade_channel
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，渠道维表，地理维表，加入维度数据。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_channel_m partition(part_date)
select 
a.channel_id                         --   '渠道id'
,a.sub_org_id                         --   '养老机构id'
,b.sub_org_name                       --   '养老机构名称'
,b.org_id                             --   '养老集团id'
,b.org_name                           --   '养老集团名称'
,d.area_id                            --   '区域id'
,d.area_name                          --   '区域名称'
,c.trade_channel_name                       --   '渠道名称'
,c.channel_type                        --   '渠道类型'
,c.channel_type_name                  --   '渠道类型名称'
,a.crm_intent_cnt                     --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                   --   '下定客户数'
,a.sign_cust_cnt                      --   '签约客户数'
,a.apply_member_cnt                   --   '预申请会籍数量'
,a.deposit_member_cnt                 --   '下定会籍数量'
,a.deposit_room_cnt                   --   '下定房间数量'
,a.sign_member_cnt                    --   '签约会籍数量'
,a.sign_room_cnt                      --   '签约房间数量'
,a.deposit_amount                     --  '下定额'
,a.sales_amount                       --  '销售额'
,a.data_month
,regexp_replace(date_sub(current_date(),1),'-','')  part_date
from 
db_dw_hr.clife_hr_dws_market_by_channel_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_trade_channel c 
on a.channel_id=c.id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 


----------------渠道 年
/**
营销机构维度，渠道维度，日期维度（年）

营销管理模块
按sub_org_id，channel_id，year分组，计算各种客户数，会籍数，房间数，金额等
 */
create table db_dw_hr.clife_hr_dws_market_by_channel_y_tmp stored as parquet as 
select
split(getSet2(concat(if(tt1.channel_id is null,'',tt1.channel_id),',',if(tt2.channel_id is null,'',tt2.channel_id),',',if(tt3.channel_id is null,'',tt3.channel_id),',',if(tt4.channel_id is null,'',tt4.channel_id),',','')),',')[1]  as channel_id                          --  '渠道id'
,split(getSet2(concat(if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',','')),',')[1]  as sub_org_id                           --  '养老机构id'
,tt4.crm_intent_cnt                      --  '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --  '下定客户数'
,tt3.sign_cust_cnt                       --  '签约客户数'
,tt1.apply_member_cnt                    --  '预申请会籍数量'
,tt2.deposit_member_cnt                  --  '下定会籍数量'
,tt2.deposit_room_cnt                    --  '下定房间数量'
,tt3.sign_member_cnt                     --  '签约会籍数量'
,tt3.sign_room_cnt                       --  '签约房间数量'
,tt2.deposit_amount                      -- '下定额'
,tt3.sales_amount                        -- '销售额'
,split(getSet2(concat(if(tt1.apply_year is null,'',tt1.apply_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt4.regist_year is null,'',tt4.regist_year),',','')),',')[1]  as data_year
from 
(select 
a.sub_org_id
,a.channel_id
,a.apply_year  
,count(distinct pre_membership_id)     as apply_member_cnt 
from 
(
select 
pre_membership_id                      --  '会籍id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,apply_time                       --  '申请时间'
,channel_id
,from_unixtime( cast(t.apply_time as int),'yyyy')  as apply_year    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.apply_year
) tt1
full join 
(select 
a.sub_org_id
,a.channel_id
,a.deposit_year
,count(distinct intention_customer_id)  as deposit_cust_cnt   
,count(distinct membership_id)     as deposit_member_cnt
,count(distinct room_id)     as deposit_room_cnt
,sum(deposit_amount)         as deposit_amount   
from 
(
select 
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,deposit_time                       --  '下定时间'
,deposit_amount                     --  '定金金额'
,channel_id
,from_unixtime( cast(t.deposit_time as int),'yyyy')  as deposit_year    -- '下定日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.deposit_year
) tt2
on tt1.sub_org_id=tt2.sub_org_id and tt1.channel_id=tt2.channel_id and tt1.apply_year=tt2.deposit_year
full join 
(select 
a.sub_org_id
,a.channel_id
,a.sign_year
,count(distinct intention_customer_id)  as sign_cust_cnt   
,count(distinct membership_id)     as sign_member_cnt
,count(distinct room_id)     as sign_room_cnt
,sum(receivable_amount)         as sales_amount   
from 
(
select 
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,sign_time                       --  '签约时间'
,receivable_amount                     --  '应收金额'
,channel_id
,from_unixtime( cast(t.sign_time as int),'yyyy')  as sign_year    -- '签约日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.sign_year
) tt3
on tt1.sub_org_id=tt3.sub_org_id and tt1.channel_id=tt3.channel_id and tt1.apply_year=tt3.sign_year
full join 
(select 
a.sub_org_id
,a.channel_id
,a.regist_year  
,count(distinct intention_customer_id)     as crm_intent_cnt 
from 
(
select 
intention_customer_id              --  '意向客户id'
,trade_channel_ids                  --  '渠道id'
,sub_org_id                         --  '养老机构id'
,regist_time                       --  '录入时间'
,channel_id
,from_unixtime( cast(t.regist_time as int),'yyyy')  as regist_year    -- '申请日期'
from 
db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(trade_channel_ids,',')) l1 as  channel_id
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
) a 
group by a.sub_org_id,a.channel_id,a.regist_year
) tt4
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_year=tt4.regist_year;


/**
营销机构维度，渠道维度，日期维度（年）
db_dw_hr.clife_hr_dws_market_by_channel_m_tmp
db_dw_hr.clife_hr_dim_admin_sub_org
db_dw_hr.clife_hr_dim_trade_channel
db_dw_hr.clife_hr_dim_geographic_info
根据上一个临时表计算出来的数据，关联组织维表，渠道维表，地理维表，加入维度数据。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_channel_y partition(part_date)
select 
a.channel_id                         --   '渠道id'
,a.sub_org_id                         --   '养老机构id'
,b.sub_org_name                       --   '养老机构名称'
,b.org_id                             --   '养老集团id'
,b.org_name                           --   '养老集团名称'
,d.area_id                            --   '区域id'
,d.area_name                          --   '区域名称'
,c.trade_channel_name                       --   '渠道名称'
,c.channel_type                        --   '渠道类型'
,c.channel_type_name                  --   '渠道类型名称'
,a.crm_intent_cnt                     --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                   --   '下定客户数'
,a.sign_cust_cnt                      --   '签约客户数'
,a.apply_member_cnt                   --   '预申请会籍数量'
,a.deposit_member_cnt                 --   '下定会籍数量'
,a.deposit_room_cnt                   --   '下定房间数量'
,a.sign_member_cnt                    --   '签约会籍数量'
,a.sign_room_cnt                      --   '签约房间数量'
,a.deposit_amount                     --  '下定额'
,a.sales_amount                       --  '销售额'
,a.data_year
,regexp_replace(date_sub(current_date(),1),'-','')  part_date
from 
db_dw_hr.clife_hr_dws_market_by_channel_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_trade_channel c 
on a.channel_id=c.id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 



--------------------销售顾问  累计

/**
营销机构维度，销售顾问维度
应该跟前面的一样，之前是根据sub_org_id累计，这里是根据销售顾问累计。

db_dw_hr.clife_hr_dwd_market
db_dw_hr.clife_hr_dim_trade_channel (渠道维表)

按sub_org_id，channel_id分组，计算各种金额，人数，会籍，房间等汇总
全量计算。
 */
insert overwrite table db_dw_hr.clife_hr_dws_market_by_adv_member_f partition(part_date)
select
 if(b1.membership_adviser_id is null,a1.membership_adviser_id,b1.membership_adviser_id) as membership_adviser_id  
,if(b1.sub_org_id is null,a1.sub_org_id,b1.sub_org_id) as sub_org_id                          --   '养老机构id',
,if(b1.sub_org_name is null,a1.sub_org_name,b1.sub_org_name) as sub_org_name                        --   '养老机构名称'
,if(b1.org_id is null,a1.org_id,b1.org_id) as org_id                              --   '养老集团id'
,if(b1.org_id is null,a1.org_id,b1.org_id) as org_name                            --   '养老集团名称'
,if(b1.area_id is null,a1.area_id,b1.area_id) as area_id                             --   '区域id'
,if(b1.area_name is null,a1.area_name,b1.area_name) as area_name                           --   '区域名称'
,if(b1.membership_adviser_name is null,a1.membership_adviser_name,b1.membership_adviser_name) as membership_adviser_name 
,a1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.crm_wait_vist_cnt                   --   '营销管理待回访客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,a1.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a1.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,b1.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,b1.deposit_cust_visit_cnt              --   '下定客户回访次数'
,b1.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a1.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a1.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,b1.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,b1.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,b1.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(select
a.sub_org_id
,a.membership_adviser_id
,max(a.membership_adviser_name)  as membership_adviser_name
,max(a.sub_org_name) sub_org_name
,max(a.org_id)    org_id
,max(org_name)    org_name
,max(area_id)     area_id
,max(area_name)   area_name
,count(distinct advisory_customer_id)  as advisory_customer_cnt     --'咨询客户数'
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
,sum(advisory_visit_cnt)  as advisory_cust_visit_cnt   --'咨询客户回访次数'
,sum(intention_visit_cnt)   as pension_intent_cust_visit_cnt  -- '客户跟踪模块意向客户回访次数'
,sum(advisory_visit_cnt)/count(advisory_customer_id)  as avg_advisory_cust_visit_cnt  --'咨询客户平均回访次数'
,sum(intention_visit_cnt)/count(distinct intention_customer_id)  as avg_pension_intent_cust_visit_cnt  --'客户跟踪模块意向客户平均回访次数'
from 
(select 
 t.advisory_customer_id
,t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,t.membership_adviser_id
,t.membership_adviser_name
,case when t.advisory_customer_id is not null then visit_cnt else 0 end advisory_visit_cnt
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0) a
group by a.sub_org_id,a.membership_adviser_id)  a1
full join
(
 select
 t1.sub_org_id
,t1.membership_adviser_id
,max(t1.membership_adviser_name)  as membership_adviser_name
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(t1.visit_cnt)   as crm_intent_cust_visit_cnt  -- '营销管理模块意向客户回访次数'
,sum(t1.visit_cnt)/count(distinct intention_customer_id)  as avg_crm_intent_cust_visit_cnt  --'营销管理模块意向客户平均回访次数'
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
,sum(deposit_cust_visit_cnt)   as deposit_cust_visit_cnt  --'下定客户回访总次数'
,sum(sign_cust_vist_cnt)   as   sign_cust_visit_cnt    -- '签约客户回访总次数'
,sum(deposit_cust_visit_cnt)/sum(deposit_cust_cnt)  as avg_deposit_cust_visit_cnt  --'下定客户平均回访次数'
,sum(sign_cust_vist_cnt)/sum(sign_cust_cnt)  as avg_sign_cust_visit_cnt  --'签约客户平均回访次数'
,count(distinct crm_wait_vist_cust)  as crm_wait_vist_cnt
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.membership_adviser_id
,b.membership_adviser_name
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.visit_cnt=0 then b.intention_customer_id else null end as crm_wait_vist_cust
from 
(
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.membership_adviser_id)   membership_adviser_id
,max(t.membership_adviser_name)   membership_adviser_name                
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.membership_adviser_id) b1 
on a1.sub_org_id=b1.sub_org_id  and a1.membership_adviser_id=b1.membership_adviser_id
left join 
(
select
t1.sub_org_id
,t1.membership_adviser_id
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select 
c.membership_id
,c.sub_org_id
,c.membership_adviser_id
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select 
 t.membership_id 
,max(t.membership_adviser_id)  membership_adviser_id 
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id)  c
) t1
group by t1.sub_org_id,t1.membership_adviser_id) c1
on b1.sub_org_id=c1.sub_org_id  and b1.membership_adviser_id=c1.membership_adviser_id
left join 
(
select
t1.sub_org_id
,t1.membership_adviser_id
,count(distinct t1.deposit_room_cnt)  deposit_room_cnt
,count(distinct t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,c.membership_adviser_id
,case when deposit_time is null then room_id else null end as deposit_room_cnt
,case when sign_time   is null then room_id else null end as sign_room_cnt
from 
(select 
 t.room_id  
,t.sub_org_id  as   sub_org_id                      --  '养老机构id' 
,t.membership_adviser_id
,t.deposit_time   as deposit_time      -- '下定时间集合'
,t.sign_time  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  c
) t1
group by t1.sub_org_id,t1.membership_adviser_id) d1
on b1.sub_org_id=d1.sub_org_id  and b1.membership_adviser_id=d1.membership_adviser_id
left join 
(select 
 sub_org_id
,membership_adviser_id
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(receivable_amount) sales_amount            --  '应收金额'
from db_dw_hr.clife_hr_dwd_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by sub_org_id,membership_adviser_id
) e1
on b1.sub_org_id=e1.sub_org_id  and b1.membership_adviser_id=e1.membership_adviser_id;




-----------------  销售顾问  日


create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_d_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.membership_adviser_id is null,'',a1.membership_adviser_id),',',if(tt1.membership_adviser_id is null,'',tt1.membership_adviser_id),',',if(tt4.membership_adviser_id is null,'',tt4.membership_adviser_id),',',if(b1.membership_adviser_id is null,'',b1.membership_adviser_id),',',if(tt2.membership_adviser_id is null,'',tt2.membership_adviser_id),',',if(tt3.membership_adviser_id is null,'',tt3.membership_adviser_id),',',if(tt5.membership_adviser_id is null,'',tt5.membership_adviser_id),',',if(tt7.membership_adviser_id is null,'',tt7.membership_adviser_id),',','')),',')[1] as membership_adviser_id                          --   '会籍顾问id',
,split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_visit_cnt as pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_visit_cnt as crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,split(getSet2(concat(if(a1.regist_date is null,'',a1.regist_date),',',if(tt1.adviser_date is null,'',tt1.adviser_date),',',if(tt4.visit_date is null,'',tt4.visit_date),',',if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt5.visit_date is null,'',tt5.visit_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',','')),',')[1]  as data_date                  -- 数据日期
from 
(select
a.sub_org_id
,membership_adviser_id
,a.regist_date
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select 
t.intention_customer_id
,t.membership_adviser_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy-MM-dd') as regist_date                        -- '意向客户录入日期'
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.regist_date)  a1
full join 
(select
a.sub_org_id
,a.membership_adviser_id
,a.adviser_date
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,from_unixtime( cast(t.adviser_time as int),'yyyy-MM-dd') as adviser_date                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.adviser_date)  tt1
on a1.sub_org_id=tt1.sub_org_id and a1.membership_adviser_id=tt1.membership_adviser_id and a1.regist_date=tt1.adviser_date
full join 
(
select
aa4.sub_org_id
,aa4.membership_adviser_id
,aa4.visit_date
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,10) as visit_date                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.membership_adviser_id,aa4.visit_date
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.membership_adviser_id=tt4.membership_adviser_id and a1.regist_date=tt4.visit_date
full join 
(
 select
 t1.sub_org_id
,t1.regist_date
,t1.membership_adviser_id
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.membership_adviser_id
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
from 
(
select 
 t.intention_customer_id
,max(t.membership_adviser_id) membership_adviser_id
,max(t.sub_org_id) sub_org_id                     
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.membership_adviser_id,t1.regist_date) b1 
on a1.sub_org_id=b1.sub_org_id  and a1.membership_adviser_id=b1.membership_adviser_id and a1.regist_date=b1.regist_date
full join 
(
select 
aa2.sub_org_id
,aa2.membership_adviser_id
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.membership_adviser_id,aa2.deposit_date
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.membership_adviser_id=tt2.membership_adviser_id and a1.regist_date=tt2.deposit_date
full join 
(
select 
aa3.sub_org_id
,aa3.sign_date
,aa3.membership_adviser_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '签约客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.membership_adviser_id,aa3.sign_date
)  tt3
on a1.sub_org_id=tt3.sub_org_id  and a1.membership_adviser_id=tt3.membership_adviser_id and a1.regist_date=tt3.sign_date
full join 
(
select
aa5.sub_org_id
,aa5.visit_date
,aa5.membership_adviser_id
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2) /count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,10) as visit_date                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt 
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
)  aa5 
group by aa5.sub_org_id,aa5.membership_adviser_id,aa5.visit_date
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.membership_adviser_id=tt5.membership_adviser_id and a1.regist_date=tt5.visit_date
full join
(
select
aa7.sub_org_id
,aa7.apply_date
,aa7.membership_adviser_id
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.membership_adviser_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.membership_adviser_id,aa7.apply_date
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_date=tt7.apply_date;



insert overwrite table db_dw_hr.clife_hr_dws_market_by_adv_member_d partition(part_date)
select
a.membership_adviser_id                --   '会籍顾问id'
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,d.area_id                             --   '区域id'
,d.area_name                           --   '区域名称'
,c.worker_name   as membership_adviser_name     -- '会籍顾问名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_adv_member_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_worker_new c
on a.membership_adviser_id=c.nurse_worker_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 


-------------------------------销售顾问 月
create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_m_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.membership_adviser_id is null,'',a1.membership_adviser_id),',',if(tt1.membership_adviser_id is null,'',tt1.membership_adviser_id),',',if(tt4.membership_adviser_id is null,'',tt4.membership_adviser_id),',',if(b1.membership_adviser_id is null,'',b1.membership_adviser_id),',',if(tt2.membership_adviser_id is null,'',tt2.membership_adviser_id),',',if(tt3.membership_adviser_id is null,'',tt3.membership_adviser_id),',',if(tt5.membership_adviser_id is null,'',tt5.membership_adviser_id),',',if(tt7.membership_adviser_id is null,'',tt7.membership_adviser_id),',','')),',')[1] as membership_adviser_id                          --   '会籍顾问id',
,split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_visit_cnt as pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_visit_cnt as crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,split(getSet2(concat(if(a1.regist_month is null,'',a1.regist_month),',',if(tt1.adviser_month is null,'',tt1.adviser_month),',',if(tt4.visit_month is null,'',tt4.visit_month),',',if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt5.visit_month is null,'',tt5.visit_month),',',if(tt7.apply_month is null,'',tt7.apply_month),',','')),',')[1]  as data_month                  -- 数据日期
from 
(select
a.sub_org_id
,membership_adviser_id
,a.regist_month
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select 
t.intention_customer_id
,t.membership_adviser_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy-MM') as regist_month                        -- '意向客户录入日期'
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.regist_month)  a1
full join 
(select
a.sub_org_id
,a.membership_adviser_id
,a.adviser_month
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,from_unixtime( cast(t.adviser_time as int),'yyyy-MM') as adviser_month                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.adviser_month)  tt1
on a1.sub_org_id=tt1.sub_org_id and a1.membership_adviser_id=tt1.membership_adviser_id and a1.regist_month=tt1.adviser_month
full join 
(
select
aa4.sub_org_id
,aa4.membership_adviser_id
,aa4.visit_month
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,7) as visit_month                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.membership_adviser_id,aa4.visit_month
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.membership_adviser_id=tt4.membership_adviser_id and a1.regist_month=tt4.visit_month
full join 
(
 select
 t1.sub_org_id
,t1.regist_month
,t1.membership_adviser_id
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.membership_adviser_id
,from_unixtime( cast(regist_time as int),'yyyy-MM')  as regist_month
from 
(
select 
 t.intention_customer_id
,max(t.membership_adviser_id) membership_adviser_id
,max(t.sub_org_id) sub_org_id                     
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.membership_adviser_id,t1.regist_month) b1 
on a1.sub_org_id=b1.sub_org_id  and a1.membership_adviser_id=b1.membership_adviser_id and a1.regist_month=b1.regist_month
full join 
(
select 
aa2.sub_org_id
,aa2.membership_adviser_id
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.membership_adviser_id,aa2.deposit_month
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.membership_adviser_id=tt2.membership_adviser_id and a1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,aa3.membership_adviser_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '签约客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.membership_adviser_id,aa3.sign_month
)  tt3
on a1.sub_org_id=tt3.sub_org_id  and a1.membership_adviser_id=tt3.membership_adviser_id and a1.regist_month=tt3.sign_month
full join 
(
select
aa5.sub_org_id
,aa5.visit_month
,aa5.membership_adviser_id
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2) /count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,7) as visit_month                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
)  aa5 
group by aa5.sub_org_id,aa5.membership_adviser_id,aa5.visit_month
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.membership_adviser_id=tt5.membership_adviser_id and a1.regist_month=tt5.visit_month
full join
(
select
aa7.sub_org_id
,aa7.apply_month
,aa7.membership_adviser_id
,count(distinct pre_membership_id) as apply_member_cnt
  from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.membership_adviser_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.membership_adviser_id,aa7.apply_month
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_month=tt7.apply_month;





insert overwrite table db_dw_hr.clife_hr_dws_market_by_adv_member_m partition(part_date)
select
a.membership_adviser_id                --   '会籍顾问id'
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,d.area_id                             --   '区域id'
,d.area_name                           --   '区域名称'
,c.worker_name   as membership_adviser_name     -- '会籍顾问名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_adv_member_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_worker_new c
on a.membership_adviser_id=c.nurse_worker_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 













------------------------------   会籍顾问  年

create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_y_tmp stored as parquet as 
select
split(getSet2(concat(if(a1.membership_adviser_id is null,'',a1.membership_adviser_id),',',if(tt1.membership_adviser_id is null,'',tt1.membership_adviser_id),',',if(tt4.membership_adviser_id is null,'',tt4.membership_adviser_id),',',if(b1.membership_adviser_id is null,'',b1.membership_adviser_id),',',if(tt2.membership_adviser_id is null,'',tt2.membership_adviser_id),',',if(tt3.membership_adviser_id is null,'',tt3.membership_adviser_id),',',if(tt5.membership_adviser_id is null,'',tt5.membership_adviser_id),',',if(tt7.membership_adviser_id is null,'',tt7.membership_adviser_id),',','')),',')[1] as membership_adviser_id                          --   '会籍顾问id',
,split(getSet2(concat(if(a1.sub_org_id is null,'',a1.sub_org_id),',',if(tt1.sub_org_id is null,'',tt1.sub_org_id),',',if(tt4.sub_org_id is null,'',tt4.sub_org_id),',',if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,tt1.advisory_customer_cnt               --   '咨询客户数'
,a1.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt4.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,tt4.pension_intent_visit_cnt as pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,tt5.crm_intent_visit_cnt as crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt4.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,tt4.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,split(getSet2(concat(if(a1.regist_year is null,'',a1.regist_year),',',if(tt1.adviser_year is null,'',tt1.adviser_year),',',if(tt4.visit_year is null,'',tt4.visit_year),',',if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt5.visit_year is null,'',tt5.visit_year),',',if(tt7.apply_year is null,'',tt7.apply_year),',','')),',')[1]  as data_year                  -- 数据日期
from 
(select
a.sub_org_id
,membership_adviser_id
,a.regist_year
,count(distinct intention_customer_id)  as pension_intent_cnt    --'客户跟踪模块意向客户数',
from
(select 
t.intention_customer_id
,t.membership_adviser_id
,t.sub_org_id                         --  '养老机构id'
,t.sub_org_name                       --  '养老机构名称'
,t.org_id                             --  '集团id'
,t.org_name                           --  '集团名称'
,t.area_id                            --  '区域id'
,t.area_name
,from_unixtime( cast(t.regist_time as int),'yyyy') as regist_year                        -- '意向客户录入日期'
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.regist_year)  a1
full join 
(select
a.sub_org_id
,a.membership_adviser_id
,a.adviser_year
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
t.advisory_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,from_unixtime( cast(t.adviser_time as int),'yyyy') as adviser_year                        -- '意向客户录入日期'
,case when t.intention_customer_id is not null then visit_cnt else 0 end intention_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
) a
group by a.sub_org_id,a.membership_adviser_id,a.adviser_year)  tt1
on a1.sub_org_id=tt1.sub_org_id and a1.membership_adviser_id=tt1.membership_adviser_id and a1.regist_year=tt1.adviser_year
full join 
(
select
aa4.sub_org_id
,aa4.membership_adviser_id
,aa4.visit_year
,count(distinct intention_customer_id)  int_cnt
,count(distinct advisory_customer_id)   adv_cnt
,sum(int_visit_cnt)  as pension_intent_visit_cnt
,sum(adv_visit_cnt)  as advisory_cust_visit_cnt
,sum(int_visit_cnt)/count(distinct intention_customer_id)   as avg_pension_intent_cust_visit_cnt
,sum(adv_visit_cnt)/count(distinct advisory_customer_id)  as avg_advisory_cust_visit_cnt
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,4) as visit_year                         -- 列开的回访时间
,t.visit_time
,t.advisory_customer_id
,case when t.visit_time is not null and visit_time2 is not null and t.intention_customer_id is not null then 1 else 0 end int_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.advisory_customer_id is not null then 1 else 0 end adv_visit_cnt
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  aa4 
group by aa4.sub_org_id,aa4.membership_adviser_id,aa4.visit_year
)  tt4
on a1.sub_org_id=tt4.sub_org_id and a1.membership_adviser_id=tt4.membership_adviser_id and a1.regist_year=tt4.visit_year
full join 
(
 select
 t1.sub_org_id
,t1.regist_year
,t1.membership_adviser_id
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.membership_adviser_id
,from_unixtime( cast(regist_time as int),'yyyy')  as regist_year
from 
(
select 
 t.intention_customer_id
,max(t.membership_adviser_id) membership_adviser_id
,max(t.sub_org_id) sub_org_id                     
,max(t.regist_time)   as regist_time
from
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id
) b
) t1
group by t1.sub_org_id,t1.membership_adviser_id,t1.regist_year) b1 
on a1.sub_org_id=b1.sub_org_id  and a1.membership_adviser_id=b1.membership_adviser_id and a1.regist_year=b1.regist_year
full join 
(
select 
aa2.sub_org_id
,aa2.membership_adviser_id
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.membership_adviser_id,aa2.deposit_year
)  tt2
on a1.sub_org_id=tt2.sub_org_id and a1.membership_adviser_id=tt2.membership_adviser_id and a1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,aa3.membership_adviser_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '签约客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,membership_adviser_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.membership_adviser_id,aa3.sign_year
)  tt3
on a1.sub_org_id=tt3.sub_org_id  and a1.membership_adviser_id=tt3.membership_adviser_id and a1.regist_year=tt3.sign_year
full join 
(
select
aa5.sub_org_id
,aa5.visit_year
,aa5.membership_adviser_id
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2) /count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,t.membership_adviser_id
,visit_time2
,substr(visit_time2,1,4) as visit_year                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
)  aa5 
group by aa5.sub_org_id,aa5.membership_adviser_id,aa5.visit_year
)  tt5
on a1.sub_org_id=tt5.sub_org_id and a1.membership_adviser_id=tt5.membership_adviser_id and a1.regist_year=tt5.visit_year
full join
(
select
aa7.sub_org_id
,aa7.apply_year
,aa7.membership_adviser_id
,count(distinct pre_membership_id) as apply_member_cnt
  from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.membership_adviser_id
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.membership_adviser_id,aa7.apply_year
) tt7
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_year=tt7.apply_year;





insert overwrite table db_dw_hr.clife_hr_dws_market_by_adv_member_y partition(part_date)
select
a.membership_adviser_id                --   '会籍顾问id'
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,d.area_id                             --   '区域id'
,d.area_name                           --   '区域名称'
,c.worker_name   as membership_adviser_name     -- '会籍顾问名称'
,a.advisory_customer_cnt               --   '咨询客户数'
,a.pension_intent_cnt                  --   '客户跟踪模块意向客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.advisory_cust_visit_cnt             --   '咨询客户回访次数'
,a.pension_intent_cust_visit_cnt       --   '客户跟踪模块意向客户回访次数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_advisory_cust_visit_cnt         --   '咨询客户平均回访次数'
,a.avg_pension_intent_cust_visit_cnt   --   '客户跟踪模块意向客户平均回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_adv_member_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_worker_new c
on a.membership_adviser_id=c.nurse_worker_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-',''); 













---------------------------  楼栋  累计
/**
按intention_customer_id分组，拼接时间
 */
create table db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp stored as parquet as 
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt     
,max(t.regist_time)   regist_time               
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_set(cast(building_id as string)))  as building_ids       -- '楼栋ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id;



/**
按membership_id分组，拼接时间。
 */
create table db_dw_hr.clife_hr_dws_crm_member_building_info_tmp stored as parquet as 
select 
 t.membership_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(building_id as string)))  as building_ids       -- '楼栋ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id;


insert overwrite table db_dw_hr.clife_hr_dws_market_by_building_f partition(part_date)
select
b1.building_id as building_id      --'楼栋'
,b1.sub_org_id as sub_org_id                          --   '养老机构id',
,b1.sub_org_name as sub_org_name                        --   '养老机构名称'
,b1.org_id as org_id                              --   '养老集团id'
,b1.org_id as org_name                            --   '养老集团名称'
,b1.area_id as area_id                             --   '区域id'
,b1.area_name as area_name                           --   '区域名称'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,c1.transfer_to_member_cnt              --   '转让会籍数'
,c1.transfer_from_member_cnt            --   '继承会籍数'
,c1.withdraw_member_cnt                 --   '退会会籍数'
,b1.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,b1.deposit_cust_visit_cnt              --   '下定客户回访次数'
,b1.sign_cust_vist_cnt as sign_cust_visit_cnt                 --   '签约客户回访次数'
,b1.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,b1.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,b1.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,e1.card_pay_amount                     --	  '刷卡金额'
,e1.cash_pay_amount                     --   '现金金额'
,e1.weixin_pay_amount                   --   '微信支付金额'
,e1.alipay_amount                       --   '阿里支付金额'	
,e1.other_pay_amount                    --   '其他方式支付金额'
,e1.received_amount                     --   '已收金额'
,e1.received_installment_amount         --   '已收分期金额'
,e1.received_occupancy_amount           --   '已收占用费'
,e1.membership_amount                   --   '会籍费'
,e1.overdue_amount                      --   '滞纳金'
,e1.received_overdue_amount             --   '已收滞纳金'
,e1.other_amount                        --   '其他费用'
,e1.manager_amount                      --   '管理费'
,e1.trust_amount                        --   '托管费' 
,e1.membership_discount_amount          --   '会籍优惠价' 
,e1.card_amount                         --   '卡费'
,e1.card_manager_amount                 --   '卡管理费'
,e1.other_transfor_to_amount            --   '其他转让费' 
,e1.transfor_to_amount                  --   '转让费'
,e1.other_transfor_from_amount          --   '其他继承费' 
,e1.transfor_from_amount                --   '继承费'
,e1.withdraw_amount                     --   '退会金额' 
,b1.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,b1.assess_cnt  as assess_cnt                          --   '评估总次数'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(
-- 按sub_org_id，building_id分组。计算各种客户数，意向客户数，下定客户数，签约客户数，回访客户数等。
 select
 t1.sub_org_id
,t1.building_id
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(t1.visit_cnt)   as crm_intent_cust_visit_cnt  -- '营销管理模块意向客户回访次数'
,sum(t1.visit_cnt)/count(distinct intention_customer_id)  as avg_crm_intent_cust_visit_cnt  --'营销管理模块意向客户平均回访次数'
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
,sum(deposit_cust_visit_cnt)   as deposit_cust_visit_cnt  --'下定客户回访总次数'
,sum(sign_cust_vist_cnt)   as   sign_cust_vist_cnt    -- '签约客户回访总次数'
,sum(deposit_cust_visit_cnt)/sum(deposit_cust_cnt)  as avg_deposit_cust_visit_cnt  --'下定客户平均回访次数'
,sum(sign_cust_vist_cnt)/sum(sign_cust_cnt)  as avg_sign_cust_visit_cnt  --'签约客户平均回访次数'
,sum(assess_cnt) assess_cnt
,sum(assess_cust_cnt)  assess_cust_cnt
 from 
(
select
-- 转换意向类型
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,b.building_id
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt
from 
(
select
-- 按楼栋id炸裂
 t.intention_customer_id
,t.sub_org_id                      --  '养老机构id'
,t.sub_org_name                    --  '养老机构名称'
,t.org_id                      --  '集团id'
,t.org_name                       --  '集团名称'
,t.area_id                       --  '区域id'
,t.area_name                       --  '区域名称'
,t.visit_cnt                       --  '回访次数'
,t.first_intention_type           --  '首次意向'
,t.intention_type           --  '当前意向' 
,t.assess_cnt                       
,t.deposit_time      -- '下定时间集合'
,t.sign_time       -- '签约时间集合'
,l1.building_id 
from db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp t 
lateral view explode(split(building_ids,','))   l1 as  building_id 
) b
) t1
group by t1.sub_org_id,t1.building_id) b1 
full join 
(
select
-- 按sub_org_id，building_id分组，汇总计算数量
t1.sub_org_id
,t1.building_id
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select
-- 转换时间为数量
c.membership_id
,c.sub_org_id
,c.building_id
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select
-- 按楼栋id炸裂，取出时间
 t.membership_id  
,t.sub_org_id                      --  '养老机构id'
,building_id
,apply_time      -- '预申请时间集合' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,transfer_to_time       -- '转让时间集合'
,transfer_from_time       -- '继承时间集合'
,withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dws_crm_member_building_info_tmp  t 
lateral view explode(split(building_ids,','))   l1 as  building_id )  c
) t1
group by t1.sub_org_id,t1.building_id) c1
on b1.sub_org_id=c1.sub_org_id  and b1.building_id=c1.building_id
left join 
(
select
-- 按sub_org_id，building_id分组，计算房间数
t1.sub_org_id
,t1.building_id
,sum(t1.deposit_room_cnt)  deposit_room_cnt
,sum(t1.sign_room_cnt)  sign_room_cnt
from 
(
select
-- 转换时间为数量
c.room_id
,c.sub_org_id
,c.building_id
,case when deposit_time is null then 1 else 0 end as deposit_room_cnt
,case when sign_time   is null then 1 else 0 end as sign_room_cnt
from 
(select
-- 按room_id，building_id分组，聚合时间
 t.room_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id' 
,t.building_id  building_id
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.room_id,t.building_id)  c
) t1
group by t1.sub_org_id,t1.building_id) d1
on b1.sub_org_id=d1.sub_org_id  and b1.building_id=d1.building_id
left join 
(select 
 sub_org_id
,building_id
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '应收金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
,sum(other_transfor_from_amount)  other_transfor_from_amount    --  '其他继承费'
,sum(transfor_from_amount)        transfor_from_amount       --  '继承费'
,sum(withdraw_amount)             withdraw_amount        --  '退会金额'
from db_dw_hr.clife_hr_dwm_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by sub_org_id,building_id
) e1
on b1.sub_org_id=e1.sub_org_id  and b1.building_id=e1.building_id;





------------------------------楼栋  日


create table if not exists db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp stored as parquet as
select 
 a.intention_customer_id --意向客户id
,max(a.name)        as name                     --客户姓名
,max(a.sex)     sex --性别
,max(a.intention_type)  as  intentional_type  --当前意向类型
,max(a.membership_adviser_id)  as membership_adviser_id   --会籍顾问id
,max(a.membership_adviser_name)  membership_adviser_name   --会籍顾问名称
,max(a.trade_channel_ids)   trade_channel_ids   --渠道id
,max(a.sub_org_id) sub_org_id
,max(a.first_intention_type) first_intention_type   -- 首次意向
,max(a.regist_time)     regist_time        --意向客户录入时间
,max(a.visit_time)      visit_time        --回访时间
,max(a.visit_cnt)       visit_cnt        --回访次数
,max(a.remind_visit_time)   remind_visit_time    --提醒回访时间
,max(a.assess_time)   assess_time         -- 评估时间
,concat_ws(',',collect_list(from_unixtime( cast(a.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time_list      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(a.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time_list       -- '签约时间集合'
,concat_ws(',',collect_list(cast(building_id as string)))  as building_ids       -- '楼栋ids'
from db_dw_hr.clife_hr_dwd_market a 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by a.intention_customer_id;


create table if not exists db_dw_hr.clife_hr_dws_market_by_building_d_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.building_id is null,'',b1.building_id),',',if(tt2.building_id is null,'',tt2.building_id),',',if(tt3.building_id is null,'',tt3.building_id),',',if(tt5.building_id is null,'',tt5.building_id),',',if(tt6.building_id is null,'',tt6.building_id),',',if(tt7.building_id is null,'',tt7.building_id),',',if(tt8.building_id is null,'',tt8.building_id),',',if(tt9.building_id is null,'',tt9.building_id),',',if(tt10.building_id is null,'',tt10.building_id),',','')),',')[1] as building_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt5.crm_intent_visit_cnt as  crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfor_to_amount            --   '其他转让费' 
,tt8.transfor_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,tt6.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,tt6.assess_cnt  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt5.visit_date is null,'',tt5.visit_date),',',if(tt6.assess_date is null,'',tt6.assess_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',','')),',')[1]  as data_date                  -- 数据日期
from  
(
 select
t1.sub_org_id
,t1.building_id
,t1.regist_date
,sum(t1.first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(t1.first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(t1.first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(t1.first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(t1.high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(t1.mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(t1.low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(t1.highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct t1.intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
intention_customer_id
,sub_org_id
,sub_org_name
,org_id
,org_name
,area_id
,area_name
,case when first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when intention_type=1 then 1 else 0 end as high_cnt
,case when intention_type=2 then 1 else 0 end as mid_cnt
,case when intention_type=3 then 1 else 0 end as low_cnt
,case when intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
,building_id
from 
db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp  as  b 
lateral view explode(split(building_ids,',')) l1 as building_id
)  as  t1
group by t1.sub_org_id,t1.building_id,t1.regist_date) as b1 
full join 
(
select 
aa2.sub_org_id
,building_id
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.building_id,aa2.deposit_date
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.building_id=tt2.building_id and b1.regist_date=tt2.deposit_date
full join 
(
select 
aa3.sub_org_id
,aa3.sign_date
,aa3.building_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.building_id,aa3.sign_date
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.building_id=tt3.building_id and b1.regist_date=tt3.sign_date
full join 
(
select
aa5.sub_org_id
,aa5.building_id
,aa5.visit_date
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2)/count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,visit_time2
,substr(visit_time2,1,10) as visit_date                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)
as  aa5 
group by aa5.sub_org_id,aa5.building_id,aa5.visit_date
) as  tt5
on b1.sub_org_id=tt5.sub_org_id and b1.building_id=tt5.building_id and b1.regist_date=tt5.visit_date
full join 
(
select 
aa6.sub_org_id
,aa6.building_id
,aa6.assess_date
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,assess_time2
,substr(assess_time2,1,10) as assess_date                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)  aa6 
group by aa6.sub_org_id,aa6.building_id,aa6.assess_date
)  tt6
on b1.sub_org_id=tt6.sub_org_id  and b1.building_id=tt6.building_id and b1.regist_date=tt6.assess_date
full join 
(
select
aa7.sub_org_id
,aa7.building_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from 
(
select 
t.pre_membership_id
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.building_id,aa7.apply_date
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.building_id=tt7.building_id and b1.regist_date=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.building_id
,aa8.transfer_to_date
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd')  transfer_to_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.building_id,aa8.transfer_to_date
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.building_id=tt8.building_id and b1.regist_date=tt8.transfer_to_date
full join 
(
select
aa9.sub_org_id
,aa9.building_id
,aa9.transfer_from_date
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount     
from
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount as transfer_from_amount            
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd')  transfer_from_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.building_id,aa9.transfer_from_date
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.building_id=tt9.building_id and b1.regist_date=tt9.transfer_from_date
full join 
(
select
aa10.sub_org_id
,aa10.building_id
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount   
from  
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd')  withdraw_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.building_id,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_date=tt10.withdraw_date;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_building_d partition(part_date)
select
a.building_id
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfor_to_amount            --  '其他转让费' 
,a.transfor_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_building_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  





--------------------------------楼栋  月


create table if not exists db_dw_hr.clife_hr_dws_market_by_building_m_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.building_id is null,'',b1.building_id),',',if(tt2.building_id is null,'',tt2.building_id),',',if(tt3.building_id is null,'',tt3.building_id),',',if(tt5.building_id is null,'',tt5.building_id),',',if(tt6.building_id is null,'',tt6.building_id),',',if(tt7.building_id is null,'',tt7.building_id),',',if(tt8.building_id is null,'',tt8.building_id),',',if(tt9.building_id is null,'',tt9.building_id),',',if(tt10.building_id is null,'',tt10.building_id),',','')),',')[1] as building_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt5.crm_intent_visit_cnt as crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfor_to_amount            --   '其他转让费' 
,tt8.transfor_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,tt6.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,tt6.assess_cnt  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt5.visit_month is null,'',tt5.visit_month),',',if(tt6.assess_month is null,'',tt6.assess_month),',',if(tt7.apply_month is null,'',tt7.apply_month),',',if(tt8.transfer_to_month is null,'',tt8.transfer_to_month),',',if(tt9.transfer_from_month is null,'',tt9.transfer_from_month),',',if(tt10.withdraw_month is null,'',tt10.withdraw_month),',','')),',')[1]  as data_month                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.building_id
,t1.regist_month
,sum(t1.first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(t1.first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(t1.first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(t1.first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(t1.high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(t1.mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(t1.low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(t1.highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct t1.intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(b.regist_time as int),'yyyy-MM')  as regist_month
,l1.building_id
from 
db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp b 
lateral view explode(split(building_ids,',')) l1 as building_id
) as t1
group by t1.sub_org_id,t1.building_id,t1.regist_month) b1 
full join 
(
select 
aa2.sub_org_id
,building_id
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.building_id,aa2.deposit_month
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.building_id=tt2.building_id and b1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,aa3.building_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.building_id,aa3.sign_month
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.building_id=tt3.building_id and b1.regist_month=tt3.sign_month
full join 
(
select
aa5.sub_org_id
,aa5.building_id
,aa5.visit_month
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2)/count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
  from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,visit_time2
,substr(visit_time2,1,7) as visit_month                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)  aa5 
group by aa5.sub_org_id,aa5.building_id,aa5.visit_month
)  tt5
on b1.sub_org_id=tt5.sub_org_id and b1.building_id=tt5.building_id and b1.regist_month=tt5.visit_month
full join 
(
select 
aa6.sub_org_id
,aa6.building_id
,aa6.assess_month
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,assess_time2
,substr(assess_time2,1,7) as assess_month                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)  aa6 
group by aa6.sub_org_id,aa6.building_id,aa6.assess_month
)  tt6
on b1.sub_org_id=tt6.sub_org_id  and b1.building_id=tt6.building_id and b1.regist_month=tt6.assess_month
full join 
(
select
aa7.sub_org_id
,aa7.building_id
,aa7.apply_month
,count(distinct pre_membership_id) as apply_member_cnt
  from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.building_id,aa7.apply_month
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.building_id=tt7.building_id and b1.regist_month=tt7.apply_month
full join 
(
select
aa8.sub_org_id
,aa8.building_id
,aa8.transfer_to_month
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM')  transfer_to_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.building_id,aa8.transfer_to_month
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.building_id=tt8.building_id and b1.regist_month=tt8.transfer_to_month
full join 
(
select
aa9.sub_org_id
,aa9.building_id
,aa9.transfer_from_month
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount
  from
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount as transfer_from_amount           
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM')  transfer_from_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.building_id,aa9.transfer_from_month
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.building_id=tt9.building_id and b1.regist_month=tt9.transfer_from_month
full join 
(
select
aa10.sub_org_id
,aa10.building_id
,aa10.withdraw_month
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount   
  from
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM')  withdraw_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.building_id,aa10.withdraw_month
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_month=tt10.withdraw_month;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_building_m partition(part_date)
select
a.building_id
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfor_to_amount            --  '其他转让费' 
,a.transfor_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_building_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  



----------楼栋  年

create table if not exists db_dw_hr.clife_hr_dws_market_by_building_y_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.building_id is null,'',b1.building_id),',',if(tt2.building_id is null,'',tt2.building_id),',',if(tt3.building_id is null,'',tt3.building_id),',',if(tt5.building_id is null,'',tt5.building_id),',',if(tt6.building_id is null,'',tt6.building_id),',',if(tt7.building_id is null,'',tt7.building_id),',',if(tt8.building_id is null,'',tt8.building_id),',',if(tt9.building_id is null,'',tt9.building_id),',',if(tt10.building_id is null,'',tt10.building_id),',','')),',')[1] as building_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt5.crm_intent_visit_cnt as crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,tt5.deposit_cust_visit_cnt              --   '下定客户回访次数'
,tt5.sign_cust_visit_cnt                 --   '签约客户回访次数'
,tt5.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,tt5.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,tt5.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额'
,tt3.card_pay_amount                     --	  '刷卡金额'
,tt3.cash_pay_amount                     --   '现金金额'
,tt3.weixin_pay_amount                   --   '微信支付金额'
,tt3.alipay_amount                       --   '阿里支付金额'	
,tt3.other_pay_amount                    --   '其他方式支付金额'
,tt3.received_amount                     --   '已收金额'
,tt3.received_installment_amount         --   '已收分期金额'
,tt3.received_occupancy_amount           --   '已收占用费'
,tt3.membership_amount                   --   '会籍费'
,tt3.overdue_amount                      --   '滞纳金'
,tt3.received_overdue_amount             --   '已收滞纳金'
,tt3.other_amount                        --   '其他费用'
,tt3.manager_amount                      --   '管理费'
,tt3.trust_amount                        --   '托管费' 
,tt3.membership_discount_amount          --   '会籍优惠价' 
,tt3.card_amount                         --   '卡费'
,tt3.card_manager_amount                 --   '卡管理费'
,tt8.other_transfor_to_amount            --   '其他转让费' 
,tt8.transfor_to_amount                  --   '转让费'
,tt9.other_transfer_from_amount          --   '其他继承费' 
,tt9.transfer_from_amount                --   '继承费'
,tt10.withdraw_amount                     --   '退会金额' 
,tt6.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,tt6.assess_cnt  as assess_cnt                          --   '评估总次数'
,split(getSet2(concat(if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt5.visit_year is null,'',tt5.visit_year),',',if(tt6.assess_year is null,'',tt6.assess_year),',',if(tt7.apply_year is null,'',tt7.apply_year),',',if(tt8.transfer_to_year is null,'',tt8.transfer_to_year),',',if(tt9.transfer_from_year is null,'',tt9.transfer_from_year),',',if(tt10.withdraw_year is null,'',tt10.withdraw_year),',','')),',')[1]  as data_year                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.building_id
,t1.regist_year
,sum(t1.first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(t1.first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(t1.first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(t1.first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(t1.high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(t1.mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(t1.low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(t1.highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct t1.intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(b.regist_time as int),'yyyy')  as regist_year
,l1.building_id
from 
db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp b 
lateral view explode(split(building_ids,',')) l1 as building_id
) as t1
group by t1.sub_org_id,t1.building_id,t1.regist_year) b1 
full join 
(
select 
aa2.sub_org_id
,building_id
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.building_id,aa2.deposit_year
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.building_id=tt2.building_id and b1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,aa3.building_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(card_pay_amount)   card_pay_amount         --	    '刷卡金额'
,sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'
,sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'
,sum(alipay_amount)     alipay_amount         --     '阿里支付金额'	
,sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,sum(received_installment_amount) received_installment_amount    --  '已收分期金额'
,sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'
,sum(membership_amount)           membership_amount     --  '会籍费'
,sum(overdue_amount)              overdue_amount    --  '滞纳金'
,sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'
,sum(other_amount)                other_amount     --  '其他费用'
,sum(manager_amount)              manager_amount       --  '管理费'
,sum(trust_amount)                trust_amount      --  '托管费'
,sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  
,sum(card_amount)                 card_amount        --  '卡费'
,sum(card_manager_amount)         card_manager_amount     --  '卡管理费'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,building_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,case when pay_type_id=0 then received_amount else null end as  card_pay_amount            --	    '刷卡金额'
,case when pay_type_id=1 then received_amount else null end as  cash_pay_amount            --     '现金金额'
,case when pay_type_id=2 then received_amount else null end as  weixin_pay_amount          --     '微信支付金额'
,case when pay_type_id=3 then received_amount else null end as  alipay_amount              --     '阿里支付金额'	
,case when pay_type_id=4 then received_amount else null end as  other_pay_amount           --     '其他方式支付金额'
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额'
,received_installment_amount   --  '已收分期金额'
,received_occupancy_amount     --  '已收占用费'
,membership_amount             --  '会籍费'
,overdue_amount                --  '滞纳金'
,received_overdue_amount       --  '已收滞纳金'
,other_amount                  --  '其他费用'
,manager_amount                --  '管理费'
,trust_amount                  --  '托管费'
,membership_discount_amount    --  '会籍优惠价'  
,card_amount                   --  '卡费'
,card_manager_amount           --  '卡管理费' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.building_id,aa3.sign_year
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.building_id=tt3.building_id and b1.regist_year=tt3.sign_year
full join 
(
select
aa5.sub_org_id
,aa5.building_id
,aa5.visit_year
,count(distinct intention_customer_id)  int_cnt
,count(visit_time2)  as crm_intent_visit_cnt
,count(visit_time2)/count(distinct intention_customer_id)   as avg_crm_intent_cust_visit_cnt
,count(distinct deposit_visit_cust_cnt)  deposit_cust_visit_cnt
,sum(deposit_visit_cnt)  as deposit_visit_cnt
,sum(deposit_visit_cnt)/count(distinct deposit_visit_cust_cnt)   as avg_deposit_cust_visit_cnt
,count(distinct sign_visit_cust_cnt)  sign_cust_visit_cnt
,sum(sign_visit_cnt)  as sign_visit_cnt
,sum(sign_visit_cnt)/count(distinct sign_visit_cust_cnt)   as avg_sign_cust_visit_cnt
  from 
(   -- 回访过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,visit_time2
,substr(visit_time2,1,4) as visit_year                         -- 列开的回访时间
,t.visit_time
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.deposit_time_list is not null then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and visit_time2 is not null and t.sign_time_list is not null then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(visit_time,',')) l1 as visit_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)  aa5 
group by aa5.sub_org_id,aa5.building_id,aa5.visit_year
)  tt5
on b1.sub_org_id=tt5.sub_org_id and b1.building_id=tt5.building_id and b1.regist_year=tt5.visit_year
full join 
(
select 
aa6.sub_org_id
,aa6.building_id
,aa6.assess_year
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,building_id
,assess_time2
,substr(assess_time2,1,4) as assess_year                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
lateral view explode(split(building_ids,',')) l2 as building_id
)  aa6 
group by aa6.sub_org_id,aa6.building_id,aa6.assess_year
)  tt6
on b1.sub_org_id=tt6.sub_org_id  and b1.building_id=tt6.building_id and b1.regist_year=tt6.assess_year
full join 
(
select
aa7.sub_org_id
,aa7.building_id
,aa7.apply_year
,count(distinct pre_membership_id) as apply_member_cnt
  from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.building_id,aa7.apply_year
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.building_id=tt7.building_id and b1.regist_year=tt7.apply_year
full join 
(
select
aa8.sub_org_id
,aa8.building_id
,aa8.transfer_to_year
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy')  transfer_to_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.building_id,aa8.transfer_to_year
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.building_id=tt8.building_id and b1.regist_year=tt8.transfer_to_year
full join 
(
select
aa9.sub_org_id
,aa9.building_id
,aa9.transfer_from_year
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount
  from
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount as transfer_from_amount           
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy')  transfer_from_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.building_id,aa9.transfer_from_year
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.building_id=tt9.building_id and b1.regist_year=tt9.transfer_from_year
full join 
(
select
aa10.sub_org_id
,aa10.building_id
,aa10.withdraw_year
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount   
  from
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.building_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy')  withdraw_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.building_id,aa10.withdraw_year
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_year=tt10.withdraw_year;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_building_y partition(part_date)
select
a.building_id
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,a.deposit_cust_visit_cnt              --   '下定客户回访次数'
,a.sign_cust_visit_cnt                 --   '签约客户回访次数'
,a.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,a.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,a.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.card_pay_amount                     --  '刷卡金额'
,a.cash_pay_amount                     --  '现金金额'
,a.weixin_pay_amount                   --  '微信支付金额'
,a.alipay_amount                       --  '阿里支付金额'	
,a.other_pay_amount                    --  '其他方式支付金额'
,a.received_amount                     --  '已收金额'
,a.other_transfor_to_amount            --  '其他转让费' 
,a.transfor_to_amount                  --  '转让费' 
,a.other_transfer_from_amount          --  '其他继承费' 
,a.transfer_from_amount                --  '继承费'
,a.withdraw_amount                     --  '退会金额' 
,a.assess_cust_cnt                     --   '评估客户数'
,a.assess_cnt                          --   '评估总次数'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_building_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  



------------------房型 累计


create table db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp stored as parquet as 
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt     
,max(t.regist_time)   regist_time               
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_set(cast(room_type as string)))  as room_types       -- '房型s'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id;




create table db_dw_hr.clife_hr_dws_crm_member_room_type_info_tmp stored as parquet as 
select 
 t.membership_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(room_type as string)))  as room_types       -- '房型s'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id;


insert overwrite table db_dw_hr.clife_hr_dws_market_by_room_type_f partition(part_date)
select
b1.room_type as room_type      --'房型'
,b1.sub_org_id as sub_org_id                          --   '养老机构id',
,b1.sub_org_name as sub_org_name                        --   '养老机构名称'
,b1.org_id as org_id                              --   '养老集团id'
,b1.org_id as org_name                            --   '养老集团名称'
,b1.area_id as area_id                             --   '区域id'
,b1.area_name as area_name                           --   '区域名称'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,c1.transfer_to_member_cnt              --   '转让会籍数'
,c1.transfer_from_member_cnt            --   '继承会籍数'
,c1.withdraw_member_cnt                 --   '退会会籍数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(
 select
 t1.sub_org_id
,t1.room_type
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,b.room_type
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt
from 
(
select 
 t.intention_customer_id
,t.sub_org_id                      --  '养老机构id'
,t.sub_org_name                    --  '养老机构名称'
,t.org_id                      --  '集团id'
,t.org_name                       --  '集团名称'
,t.area_id                       --  '区域id'
,t.area_name                       --  '区域名称'
,t.visit_cnt                       --  '回访次数'
,t.first_intention_type           --  '首次意向'
,t.intention_type           --  '当前意向' 
,t.assess_cnt                       
,t.deposit_time      -- '下定时间集合'
,t.sign_time       -- '签约时间集合'
,l1.room_type 
from db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp t 
lateral view explode(split(room_types,','))   l1 as  room_type 
) b
) t1
group by t1.sub_org_id,t1.room_type) b1 
full join 
(
select
t1.sub_org_id
,t1.room_type
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select 
c.membership_id
,c.sub_org_id
,c.room_type
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select 
 t.membership_id  
,t.sub_org_id                      --  '养老机构id'
,room_type
,apply_time      -- '预申请时间集合' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,transfer_to_time       -- '转让时间集合'
,transfer_from_time       -- '继承时间集合'
,withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dws_crm_member_room_type_info_tmp  t 
lateral view explode(split(room_types,','))   l1 as  room_type )  c
) t1
group by t1.sub_org_id,t1.room_type) c1
on b1.sub_org_id=c1.sub_org_id  and b1.room_type=c1.room_type
left join 
(
select
t1.sub_org_id
,t1.room_type
,sum(t1.deposit_room_cnt)  deposit_room_cnt
,sum(t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,c.room_type
,case when deposit_time is null then 1 else 0 end as deposit_room_cnt
,case when sign_time   is null then 1 else 0 end as sign_room_cnt
from 
(select 
 t.room_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id' 
,max(t.room_type)  room_type
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.room_id,t.room_type)  c
) t1
group by t1.sub_org_id,t1.room_type) d1
on b1.sub_org_id=d1.sub_org_id  and b1.room_type=d1.room_type
left join 
(select 
 sub_org_id
,room_type
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(receivable_amount) sales_amount            --  '应收金额'
,sum(received_amount)   received_amount            --  '已收金额'
from db_dw_hr.clife_hr_dwm_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by sub_org_id,room_type
) e1
on b1.sub_org_id=e1.sub_org_id  and b1.room_type=e1.room_type;



-----------------------  房型   日



create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_d_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.room_type is null,'',b1.room_type),',',if(tt2.room_type is null,'',tt2.room_type),',',if(tt3.room_type is null,'',tt3.room_type),',',if(tt7.room_type is null,'',tt7.room_type),',',if(tt8.room_type is null,'',tt8.room_type),',',if(tt9.room_type is null,'',tt9.room_type),',',if(tt10.room_type is null,'',tt10.room_type),',','')),',')[1] as room_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',','')),',')[1]  as data_date                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.room_type
,t1.regist_date
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
,b.room_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,room_type
from
db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp t 
lateral view explode(split(room_types,',')) l1 as room_type
) b
) t1
group by t1.sub_org_id,t1.room_type,t1.regist_date) b1 
full join 
(
select 
aa2.sub_org_id
,room_type
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.room_type,aa2.deposit_date
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.room_type=tt2.room_type and b1.regist_date=tt2.deposit_date
full join 
(
select 
aa3.sub_org_id
,aa3.sign_date
,aa3.room_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.room_type,aa3.sign_date
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.room_type=tt3.room_type and b1.regist_date=tt3.sign_date
full join 
(
select
aa7.sub_org_id
,aa7.room_type
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.room_type,aa7.apply_date
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.room_type=tt7.room_type and b1.regist_date=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.room_type
,aa8.transfer_to_date
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd')  transfer_to_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.room_type,aa8.transfer_to_date
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.room_type=tt8.room_type and b1.regist_date=tt8.transfer_to_date
full join 
(
select
aa9.sub_org_id
,aa9.room_type
,aa9.transfer_from_date
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount  
from   
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as  transfer_from_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd')  transfer_from_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.room_type,aa9.transfer_from_date
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.room_type=tt9.room_type and b1.regist_date=tt9.transfer_from_date
full join 
(
select
aa10.sub_org_id
,aa10.room_type
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount  
from   
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd')  withdraw_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.room_type,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_date=tt10.withdraw_date;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_room_type_d partition(part_date)
select
 a.room_type                           --   '房型'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_room_type_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  


------------------- 房型  月

create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_m_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.room_type is null,'',b1.room_type),',',if(tt2.room_type is null,'',tt2.room_type),',',if(tt3.room_type is null,'',tt3.room_type),',',if(tt7.room_type is null,'',tt7.room_type),',',if(tt8.room_type is null,'',tt8.room_type),',',if(tt9.room_type is null,'',tt9.room_type),',',if(tt10.room_type is null,'',tt10.room_type),',','')),',')[1] as room_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt7.apply_month is null,'',tt7.apply_month),',',if(tt8.transfer_to_month is null,'',tt8.transfer_to_month),',',if(tt9.transfer_from_month is null,'',tt9.transfer_from_month),',',if(tt10.withdraw_month is null,'',tt10.withdraw_month),',','')),',')[1]  as data_month                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.room_type
,t1.regist_month
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM')  as regist_month
,b.room_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,room_type
from
db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp t 
lateral view explode(split(room_types,',')) l1 as room_type
) b
) t1
group by t1.sub_org_id,t1.room_type,t1.regist_month) b1 
full join 
(
select 
aa2.sub_org_id
,room_type
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.room_type,aa2.deposit_month
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.room_type=tt2.room_type and b1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,aa3.room_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.room_type,aa3.sign_month
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.room_type=tt3.room_type and b1.regist_month=tt3.sign_month
full join 
(
select
aa7.sub_org_id
,aa7.room_type
,aa7.apply_month
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.room_type,aa7.apply_month
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.room_type=tt7.room_type and b1.regist_month=tt7.apply_month
full join 
(
select
aa8.sub_org_id
,aa8.room_type
,aa8.transfer_to_month
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM')  transfer_to_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.room_type,aa8.transfer_to_month
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.room_type=tt8.room_type and b1.regist_month=tt8.transfer_to_month
full join 
(
select
aa9.sub_org_id
,aa9.room_type
,aa9.transfer_from_month
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount  
from   
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as  transfer_from_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM')  transfer_from_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.room_type,aa9.transfer_from_month
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.room_type=tt9.room_type and b1.regist_month=tt9.transfer_from_month
full join 
(
select
aa10.sub_org_id
,aa10.room_type
,aa10.withdraw_month
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount  
from   
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM')  withdraw_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.room_type,aa10.withdraw_month
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_month=tt10.withdraw_month;






insert overwrite table db_dw_hr.clife_hr_dws_market_by_room_type_m partition(part_date)
select
 a.room_type                           --   '房型'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_room_type_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  



--------------------- 房型  年


create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_y_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.room_type is null,'',b1.room_type),',',if(tt2.room_type is null,'',tt2.room_type),',',if(tt3.room_type is null,'',tt3.room_type),',',if(tt7.room_type is null,'',tt7.room_type),',',if(tt8.room_type is null,'',tt8.room_type),',',if(tt9.room_type is null,'',tt9.room_type),',',if(tt10.room_type is null,'',tt10.room_type),',','')),',')[1] as room_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt7.apply_year is null,'',tt7.apply_year),',',if(tt8.transfer_to_year is null,'',tt8.transfer_to_year),',',if(tt9.transfer_from_year is null,'',tt9.transfer_from_year),',',if(tt10.withdraw_year is null,'',tt10.withdraw_year),',','')),',')[1]  as data_year                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.room_type
,t1.regist_year
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy')  as regist_year
,b.room_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,room_type
from
db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp t 
lateral view explode(split(room_types,',')) l1 as room_type
) b
) t1
group by t1.sub_org_id,t1.room_type,t1.regist_year) b1 
full join 
(
select 
aa2.sub_org_id
,room_type
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.room_type,aa2.deposit_year
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.room_type=tt2.room_type and b1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,aa3.room_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,room_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.room_type,aa3.sign_year
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.room_type=tt3.room_type and b1.regist_year=tt3.sign_year
full join 
(
select
aa7.sub_org_id
,aa7.room_type
,aa7.apply_year
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.room_type,aa7.apply_year
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.room_type=tt7.room_type and b1.regist_year=tt7.apply_year
full join 
(
select
aa8.sub_org_id
,aa8.room_type
,aa8.transfer_to_year
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy')  transfer_to_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.room_type,aa8.transfer_to_year
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.room_type=tt8.room_type and b1.regist_year=tt8.transfer_to_year
full join 
(
select
aa9.sub_org_id
,aa9.room_type
,aa9.transfer_from_year
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount  
from   
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as  transfer_from_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy')  transfer_from_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.room_type,aa9.transfer_from_year
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.room_type=tt9.room_type and b1.regist_year=tt9.transfer_from_year
full join 
(
select
aa10.sub_org_id
,aa10.room_type
,aa10.withdraw_year
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount  
from   
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.room_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy')  withdraw_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.room_type,aa10.withdraw_year
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_year=tt10.withdraw_year;






insert overwrite table db_dw_hr.clife_hr_dws_market_by_room_type_y partition(part_date)
select
 a.room_type                           --   '房型'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_room_type_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  



--------------------  会籍卡  累计

create table db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp stored as parquet as 
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt     
,max(t.regist_time)   regist_time               
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_set(cast(card_id as string)))  as card_ids       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id;




create table db_dw_hr.clife_hr_dws_crm_member_card_info_tmp stored as parquet as 
select 
 t.membership_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(card_id as string)))  as card_ids       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id;



create table db_dw_hr.clife_hr_dws_crm_room_card_info_tmp stored as parquet as 
select 
 t.room_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(card_id as string)))  as card_ids       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.room_id;


insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_f partition(part_date)
select
b1.card_id as card_id      --'会籍卡id'
,b1.sub_org_id as sub_org_id                          --   '养老机构id',
,b1.sub_org_name as sub_org_name                        --   '养老机构名称'
,b1.org_id as org_id                              --   '养老集团id'
,b1.org_id as org_name                            --   '养老集团名称'
,b1.area_id as area_id                             --   '区域id'
,b1.area_name as area_name                           --   '区域名称'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,c1.transfer_to_member_cnt              --   '转让会籍数'
,c1.transfer_from_member_cnt            --   '继承会籍数'
,c1.withdraw_member_cnt                 --   '退会会籍数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(
 select
 t1.sub_org_id
,t1.card_id
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,b.card_id
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt
from 
(
select 
 t.intention_customer_id
,t.sub_org_id                      --  '养老机构id'
,t.sub_org_name                    --  '养老机构名称'
,t.org_id                      --  '集团id'
,t.org_name                       --  '集团名称'
,t.area_id                       --  '区域id'
,t.area_name                       --  '区域名称'
,t.visit_cnt                       --  '回访次数'
,t.first_intention_type           --  '首次意向'
,t.intention_type           --  '当前意向' 
,t.assess_cnt                       
,t.deposit_time      -- '下定时间集合'
,t.sign_time       -- '签约时间集合'
,l1.card_id 
from db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp t 
lateral view explode(split(card_ids,','))   l1 as  card_id 
) b
) t1
group by t1.sub_org_id,t1.card_id) b1 
full join 
(
select
t1.sub_org_id
,t1.card_id
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select 
c.membership_id
,c.sub_org_id
,c.card_id
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select 
 t.membership_id  
,t.sub_org_id                      --  '养老机构id'
,card_id
,apply_time      -- '预申请时间集合' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,transfer_to_time       -- '转让时间集合'
,transfer_from_time       -- '继承时间集合'
,withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dws_crm_member_card_info_tmp  t 
lateral view explode(split(card_ids,','))   l1 as  card_id )  c
) t1
group by t1.sub_org_id,t1.card_id) c1
on b1.sub_org_id=c1.sub_org_id  and b1.card_id=c1.card_id
left join 
(
select
t1.sub_org_id
,t1.card_id
,count(distinct t1.deposit_room_cnt)  deposit_room_cnt
,count(distinct t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,c.card_id
,case when deposit_time is null then room_id else null end as deposit_room_cnt
,case when sign_time   is null then room_id else null end as sign_room_cnt
from 
(select 
 t.room_id  
,sub_org_id                      --  '养老机构id' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,card_id
from 
db_dw_hr.clife_hr_dws_crm_room_card_info_tmp t 
lateral view explode(split(card_ids,','))   l1 as  card_id)  c
) t1
group by t1.sub_org_id,t1.card_id) d1
on b1.sub_org_id=d1.sub_org_id  and b1.card_id=d1.card_id
left join 
(select 
 sub_org_id
,card_id
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(receivable_amount) sales_amount            --  '应收金额'
,sum(received_amount)   received_amount            --  '已收金额'
from db_dw_hr.clife_hr_dwm_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by sub_org_id,card_id
) e1
on b1.sub_org_id=e1.sub_org_id  and b1.card_id=e1.card_id;




---------------------------  会籍卡 日

create table if not exists db_dw_hr.clife_hr_dws_market_by_card_d_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_id is null,'',b1.card_id),',',if(tt2.card_id is null,'',tt2.card_id),',',if(tt3.card_id is null,'',tt3.card_id),',',if(tt7.card_id is null,'',tt7.card_id),',',if(tt8.card_id is null,'',tt8.card_id),',',if(tt9.card_id is null,'',tt9.card_id),',',if(tt10.card_id is null,'',tt10.card_id),',','')),',')[1] as card_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',','')),',')[1]  as data_date                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_id
,t1.regist_date
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
,b.card_id
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_id
from
db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp t 
lateral view explode(split(card_ids,',')) l1 as card_id
) b
) t1
group by t1.sub_org_id,t1.card_id,t1.regist_date) b1 
full join 
(
select 
aa2.sub_org_id
,card_id
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_id,aa2.deposit_date
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_id=tt2.card_id and b1.regist_date=tt2.deposit_date
full join 
(
select 
aa3.sub_org_id
,aa3.sign_date
,aa3.card_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_id,aa3.sign_date
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_id=tt3.card_id and b1.regist_date=tt3.sign_date
full join 
(
select
aa7.sub_org_id
,aa7.card_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_id,aa7.apply_date
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_id=tt7.card_id and b1.regist_date=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.card_id
,aa8.transfer_to_date
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd')  transfer_to_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_id,aa8.transfer_to_date
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_id=tt8.card_id and b1.regist_date=tt8.transfer_to_date
full join 
(
select
aa9.sub_org_id
,aa9.card_id
,aa9.transfer_from_date
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from  
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd')  transfer_from_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_id,aa9.transfer_from_date
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_id=tt9.card_id and b1.regist_date=tt9.transfer_from_date
full join 
(
select
aa10.sub_org_id
,aa10.card_id
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount
from      
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd')  withdraw_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_id,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_date=tt10.withdraw_date;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_d partition(part_date)
select
 a.card_id                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  




-----------------------  会籍卡  月

create table if not exists db_dw_hr.clife_hr_dws_market_by_card_m_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_id is null,'',b1.card_id),',',if(tt2.card_id is null,'',tt2.card_id),',',if(tt3.card_id is null,'',tt3.card_id),',',if(tt7.card_id is null,'',tt7.card_id),',',if(tt8.card_id is null,'',tt8.card_id),',',if(tt9.card_id is null,'',tt9.card_id),',',if(tt10.card_id is null,'',tt10.card_id),',','')),',')[1] as card_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt7.apply_month is null,'',tt7.apply_month),',',if(tt8.transfer_to_month is null,'',tt8.transfer_to_month),',',if(tt9.transfer_from_month is null,'',tt9.transfer_from_month),',',if(tt10.withdraw_month is null,'',tt10.withdraw_month),',','')),',')[1]  as data_month                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_id
,t1.regist_month
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM')  as regist_month
,b.card_id
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_id
from
db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp t 
lateral view explode(split(card_ids,',')) l1 as card_id
) b
) t1
group by t1.sub_org_id,t1.card_id,t1.regist_month) b1 
full join 
(
select 
aa2.sub_org_id
,card_id
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_id,aa2.deposit_month
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_id=tt2.card_id and b1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,aa3.card_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_id,aa3.sign_month
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_id=tt3.card_id and b1.regist_month=tt3.sign_month
full join 
(
select
aa7.sub_org_id
,aa7.card_id
,aa7.apply_month
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_id,aa7.apply_month
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_id=tt7.card_id and b1.regist_month=tt7.apply_month
full join 
(
select
aa8.sub_org_id
,aa8.card_id
,aa8.transfer_to_month
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM')  transfer_to_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_id,aa8.transfer_to_month
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_id=tt8.card_id and b1.regist_month=tt8.transfer_to_month
full join 
(
select
aa9.sub_org_id
,aa9.card_id
,aa9.transfer_from_month
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from  
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM')  transfer_from_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_id,aa9.transfer_from_month
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_id=tt9.card_id and b1.regist_month=tt9.transfer_from_month
full join 
(
select
aa10.sub_org_id
,aa10.card_id
,aa10.withdraw_month
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount
from      
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM')  withdraw_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_id,aa10.withdraw_month
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_month=tt10.withdraw_month;











insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_m partition(part_date)
select
 a.card_id                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  









------------------------会籍卡 年



create table if not exists db_dw_hr.clife_hr_dws_market_by_card_y_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_id is null,'',b1.card_id),',',if(tt2.card_id is null,'',tt2.card_id),',',if(tt3.card_id is null,'',tt3.card_id),',',if(tt7.card_id is null,'',tt7.card_id),',',if(tt8.card_id is null,'',tt8.card_id),',',if(tt9.card_id is null,'',tt9.card_id),',',if(tt10.card_id is null,'',tt10.card_id),',','')),',')[1] as card_id
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt7.apply_year is null,'',tt7.apply_year),',',if(tt8.transfer_to_year is null,'',tt8.transfer_to_year),',',if(tt9.transfer_from_year is null,'',tt9.transfer_from_year),',',if(tt10.withdraw_year is null,'',tt10.withdraw_year),',','')),',')[1]  as data_year                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_id
,t1.regist_year
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy')  as regist_year
,b.card_id
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_id
from
db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp t 
lateral view explode(split(card_ids,',')) l1 as card_id
) b
) t1
group by t1.sub_org_id,t1.card_id,t1.regist_year) b1 
full join 
(
select 
aa2.sub_org_id
,card_id
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_id,aa2.deposit_year
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_id=tt2.card_id and b1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,aa3.card_id
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_id
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_id,aa3.sign_year
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_id=tt3.card_id and b1.regist_year=tt3.sign_year
full join 
(
select
aa7.sub_org_id
,aa7.card_id
,aa7.apply_year
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_id,aa7.apply_year
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_id=tt7.card_id and b1.regist_year=tt7.apply_year
full join 
(
select
aa8.sub_org_id
,aa8.card_id
,aa8.transfer_to_year
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_to_time as int),'yyyy')  transfer_to_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_id,aa8.transfer_to_year
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_id=tt8.card_id and b1.regist_year=tt8.transfer_to_year
full join 
(
select
aa9.sub_org_id
,aa9.card_id
,aa9.transfer_from_year
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from  
(
select 
t.membership_id
,t.other_transfor_from_amount  as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.transfor_from_time as int),'yyyy')  transfer_from_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_id,aa9.transfer_from_year
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_id=tt9.card_id and b1.regist_year=tt9.transfer_from_year
full join 
(
select
aa10.sub_org_id
,aa10.card_id
,aa10.withdraw_year
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount
from      
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_id
,from_unixtime( cast(t.withdraw_time as int),'yyyy')  withdraw_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_id,aa10.withdraw_year
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_year=tt10.withdraw_year;











insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_y partition(part_date)
select
 a.card_id                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  










---------------------- 卡类型   累计

create table db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp stored as parquet as 
select 
 t.intention_customer_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
,max(t.visit_cnt)   visit_cnt                       --  '回访次数'
,max(t.first_intention_type)    first_intention_type           --  '首次意向'
,max(t.intention_type)          intention_type           --  '当前意向' 
,max(t.assess_cnt)    assess_cnt     
,max(t.regist_time)   regist_time               
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_set(cast(card_type  as string)))  as card_types       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.intention_customer_id;




create table db_dw_hr.clife_hr_dws_crm_member_card_type_info_tmp stored as parquet as 
select 
 t.membership_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(card_type  as string)))  as card_types       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.membership_id;



create table db_dw_hr.clife_hr_dws_crm_room_card_type_info_tmp stored as parquet as 
select 
 t.room_id  
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,concat_ws(',',collect_list(from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd  HH:mm:ss')))   as apply_time      -- '预申请时间集合' 
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_to_time       -- '转让时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd  HH:mm:ss')))  as transfer_from_time       -- '继承时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd  HH:mm:ss')))  as withdraw_time       -- '退会时间集合'
,concat_ws(',',collect_set(cast(card_type  as string)))  as card_types       -- '会籍卡ids'
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by t.room_id;


insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_type_f partition(part_date)
select
b1.card_type as card_type      --'会籍卡id'
,b1.sub_org_id as sub_org_id                          --   '养老机构id',
,b1.sub_org_name as sub_org_name                        --   '养老机构名称'
,b1.org_id as org_id                              --   '养老集团id'
,b1.org_id as org_name                            --   '养老集团名称'
,b1.area_id as area_id                             --   '区域id'
,b1.area_name as area_name                           --   '区域名称'
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,b1.deposit_cust_cnt                    --   '下定客户数'
,b1.sign_cust_cnt                       --   '签约客户数'
,c1.transfer_to_member_cnt              --   '转让会籍数'
,c1.transfer_from_member_cnt            --   '继承会籍数'
,c1.withdraw_member_cnt                 --   '退会会籍数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,d1.sign_room_cnt                       --   '签约房间数量'
,e1.deposit_amount                      --   '下定额'
,e1.sales_amount                        --   '销售额'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
(
 select
 t1.sub_org_id
,t1.card_type
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id)    org_id
,max(t1.org_name)    org_name
,max(t1.area_id)     area_id
,max(t1.area_name)   area_name
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
,sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'
,sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,b.visit_cnt
,b.assess_cnt
,b.card_type
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt
from 
(
select 
 t.intention_customer_id
,t.sub_org_id                      --  '养老机构id'
,t.sub_org_name                    --  '养老机构名称'
,t.org_id                      --  '集团id'
,t.org_name                       --  '集团名称'
,t.area_id                       --  '区域id'
,t.area_name                       --  '区域名称'
,t.visit_cnt                       --  '回访次数'
,t.first_intention_type           --  '首次意向'
,t.intention_type           --  '当前意向' 
,t.assess_cnt                       
,t.deposit_time      -- '下定时间集合'
,t.sign_time       -- '签约时间集合'
,l1.card_type 
from db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp t 
lateral view explode(split(card_types,','))   l1 as  card_type 
) b
) t1
group by t1.sub_org_id,t1.card_type) b1 
full join 
(
select
t1.sub_org_id
,t1.card_type
,sum(t1.apply_member_cnt)  apply_member_cnt
,sum(t1.deposit_member_cnt)  deposit_member_cnt
,sum(t1.sign_member_cnt)  sign_member_cnt
,sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt
,sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt
,sum(t1.withdraw_member_cnt)   withdraw_member_cnt
from 
(
select 
c.membership_id
,c.sub_org_id
,c.card_type
,case when apply_time   is null then 1 else 0 end as apply_member_cnt
,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
,case when sign_time   is null then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
from 
(select 
 t.membership_id  
,t.sub_org_id                      --  '养老机构id'
,card_type
,apply_time      -- '预申请时间集合' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,transfer_to_time       -- '转让时间集合'
,transfer_from_time       -- '继承时间集合'
,withdraw_time       -- '退会时间集合'
from 
db_dw_hr.clife_hr_dws_crm_member_card_type_info_tmp  t 
lateral view explode(split(card_types,','))   l1 as  card_type )  c
) t1
group by t1.sub_org_id,t1.card_type) c1
on b1.sub_org_id=c1.sub_org_id  and b1.card_type=c1.card_type
left join 
(
select
t1.sub_org_id
,t1.card_type
,count(distinct t1.deposit_room_cnt)  deposit_room_cnt
,count(distinct t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,c.card_type
,case when deposit_time is null then room_id else null end as deposit_room_cnt
,case when sign_time   is null then room_id else null end as sign_room_cnt
from 
(select 
 t.room_id  
,sub_org_id                      --  '养老机构id' 
,deposit_time      -- '下定时间集合'
,sign_time       -- '签约时间集合'
,card_type
from 
db_dw_hr.clife_hr_dws_crm_room_card_type_info_tmp t 
lateral view explode(split(card_types,','))   l1 as  card_type)  c
) t1
group by t1.sub_org_id,t1.card_type) d1
on b1.sub_org_id=d1.sub_org_id  and b1.card_type=d1.card_type
left join 
(select 
 sub_org_id
,ca.type as card_type
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(receivable_amount) sales_amount            --  '应收金额'
,sum(received_amount)   received_amount            --  '已收金额'
from db_dw_hr.clife_hr_dwm_market t 
left join db_dw_hr.clife_hr_dim_card ca
on t.card_id=ca.card_id and ca.part_date=regexp_replace(date_sub(current_date(),1),'-','')
 where t.part_date= regexp_replace(date_sub(current_date(),1),'-','')
group by t.sub_org_id,ca.type
) e1
on b1.sub_org_id=e1.sub_org_id  and b1.card_type=e1.card_type;




---------------------------  会籍卡类型 日

create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_d_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_type is null,'',b1.card_type),',',if(tt2.card_type is null,'',tt2.card_type),',',if(tt3.card_type is null,'',tt3.card_type),',',if(tt7.card_type is null,'',tt7.card_type),',',if(tt8.card_type is null,'',tt8.card_type),',',if(tt9.card_type is null,'',tt9.card_type),',',if(tt10.card_type is null,'',tt10.card_type),',','')),',')[1] as card_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',','')),',')[1]  as data_date                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_type
,t1.regist_date
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM-dd')  as regist_date
,b.card_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_type
from
db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp t 
lateral view explode(split(card_types,',')) l1 as card_type
) b
) t1
group by t1.sub_org_id,t1.card_type,t1.regist_date) b1 
full join 
(
select 
aa2.sub_org_id
,card_type
,aa2.deposit_date
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM-dd')  as deposit_date 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_type,aa2.deposit_date
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_type=tt2.card_type and b1.regist_date=tt2.deposit_date
full join 
(
select 
aa3.sub_org_id
,aa3.sign_date
,aa3.card_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM-dd')  as sign_date
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_type,aa3.sign_date
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_type=tt3.card_type and b1.regist_date=tt3.sign_date
full join 
(
select
aa7.sub_org_id
,aa7.card_type
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from 
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.apply_time as int),'yyyy-MM-dd')  apply_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_type,aa7.apply_date
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_type=tt7.card_type and b1.regist_date=tt7.apply_date
full join 
(
select
aa8.sub_org_id
,aa8.card_type
,aa8.transfer_to_date
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from 
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM-dd')  transfer_to_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_type,aa8.transfer_to_date
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_type=tt8.card_type and b1.regist_date=tt8.transfer_to_date
full join 
(
select
aa9.sub_org_id
,aa9.card_type
,aa9.transfer_from_date
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from   
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM-dd')  transfer_from_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_type,aa9.transfer_from_date
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_type=tt9.card_type and b1.regist_date=tt9.transfer_from_date
full join 
(
select
aa10.sub_org_id
,aa10.card_type
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount     
from 
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM-dd')  withdraw_date
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_type,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_date=tt10.withdraw_date;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_type_d partition(part_date)
select
 a.card_type                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_date                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_type_d_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  




-----------------------  会籍卡类型  月

create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_m_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_type is null,'',b1.card_type),',',if(tt2.card_type is null,'',tt2.card_type),',',if(tt3.card_type is null,'',tt3.card_type),',',if(tt7.card_type is null,'',tt7.card_type),',',if(tt8.card_type is null,'',tt8.card_type),',',if(tt9.card_type is null,'',tt9.card_type),',',if(tt10.card_type is null,'',tt10.card_type),',','')),',')[1] as card_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_month is null,'',b1.regist_month),',',if(tt2.deposit_month is null,'',tt2.deposit_month),',',if(tt3.sign_month is null,'',tt3.sign_month),',',if(tt7.apply_month is null,'',tt7.apply_month),',',if(tt8.transfer_to_month is null,'',tt8.transfer_to_month),',',if(tt9.transfer_from_month is null,'',tt9.transfer_from_month),',',if(tt10.withdraw_month is null,'',tt10.withdraw_month),',','')),',')[1]  as data_month                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_type
,t1.regist_month
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy-MM')  as regist_month
,b.card_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_type
from
db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp t 
lateral view explode(split(card_types,',')) l1 as card_type
) b
) t1
group by t1.sub_org_id,t1.card_type,t1.regist_month) b1 
full join 
(
select 
aa2.sub_org_id
,card_type
,aa2.deposit_month
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy-MM')  as deposit_month 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_type,aa2.deposit_month
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_type=tt2.card_type and b1.regist_month=tt2.deposit_month
full join 
(
select 
aa3.sub_org_id
,aa3.sign_month
,aa3.card_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy-MM')  as sign_month
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_type,aa3.sign_month
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_type=tt3.card_type and b1.regist_month=tt3.sign_month
full join 
(
select
aa7.sub_org_id
,aa7.card_type
,aa7.apply_month
,count(distinct pre_membership_id) as apply_member_cnt
from 
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.apply_time as int),'yyyy-MM')  apply_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_type,aa7.apply_month
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_type=tt7.card_type and b1.regist_month=tt7.apply_month
full join 
(
select
aa8.sub_org_id
,aa8.card_type
,aa8.transfer_to_month
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from 
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-MM')  transfer_to_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_type,aa8.transfer_to_month
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_type=tt8.card_type and b1.regist_month=tt8.transfer_to_month
full join 
(
select
aa9.sub_org_id
,aa9.card_type
,aa9.transfer_from_month
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from   
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-MM')  transfer_from_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_type,aa9.transfer_from_month
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_type=tt9.card_type and b1.regist_month=tt9.transfer_from_month
full join 
(
select
aa10.sub_org_id
,aa10.card_type
,aa10.withdraw_month
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount     
from 
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy-MM')  withdraw_month
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_type,aa10.withdraw_month
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_month=tt10.withdraw_month;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_type_m partition(part_date)
select
 a.card_type                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_month                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_type_m_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  




------------------------会籍卡类型 年



create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_y_tmp stored as parquet as 
select
split(getSet2(concat(if(b1.card_type is null,'',b1.card_type),',',if(tt2.card_type is null,'',tt2.card_type),',',if(tt3.card_type is null,'',tt3.card_type),',',if(tt7.card_type is null,'',tt7.card_type),',',if(tt8.card_type is null,'',tt8.card_type),',',if(tt9.card_type is null,'',tt9.card_type),',',if(tt10.card_type is null,'',tt10.card_type),',','')),',')[1] as card_type
,split(getSet2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
,b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,b1.crm_intent_cnt                      --   '营销管理模块意向客户数'
,tt2.deposit_cust_cnt                    --   '下定客户数'
,tt3.sign_cust_cnt                       --   '签约客户数'
,tt8.transfer_to_member_cnt              --   '转让会籍数'
,tt9.transfer_from_member_cnt            --   '继承会籍数'
,tt10.withdraw_member_cnt                 --   '退会会籍数'
,tt7.apply_member_cnt                    --   '预申请会籍数量'
,tt2.deposit_member_cnt                  --   '下定会籍数量'
,tt2.deposit_room_cnt                    --   '下定房间数量'
,tt3.sign_member_cnt                     --   '签约会籍数量'
,tt3.sign_room_cnt                       --   '签约房间数量'
,tt2.deposit_amount                      --   '下定额'
,tt3.sales_amount                        --   '销售额' 
,split(getSet2(concat(if(b1.regist_year is null,'',b1.regist_year),',',if(tt2.deposit_year is null,'',tt2.deposit_year),',',if(tt3.sign_year is null,'',tt3.sign_year),',',if(tt7.apply_year is null,'',tt7.apply_year),',',if(tt8.transfer_to_year is null,'',tt8.transfer_to_year),',',if(tt9.transfer_from_year is null,'',tt9.transfer_from_year),',',if(tt10.withdraw_year is null,'',tt10.withdraw_year),',','')),',')[1]  as data_year                  -- 数据日期
from  
(
 select
 t1.sub_org_id
,t1.card_type
,t1.regist_year
,sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'
,sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'
,sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'
,sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'
,sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'
,sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'
,sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'
,sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',
 from 
(
select 
 b.intention_customer_id
,b.sub_org_id
,b.sub_org_name
,b.org_id
,b.org_name
,b.area_id
,b.area_name
,case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt
,case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt
,case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt
,case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt
,case when b.intention_type=1 then 1 else 0 end as high_cnt
,case when b.intention_type=2 then 1 else 0 end as mid_cnt
,case when b.intention_type=3 then 1 else 0 end as low_cnt
,case when b.intention_type=4 then 1 else 0 end as highest_cnt
,from_unixtime( cast(regist_time as int),'yyyy')  as regist_year
,b.card_type
from 
(
select 
 t.intention_customer_id
,sub_org_id                      --  '养老机构id'
,sub_org_name                    --  '养老机构名称'
,org_id                      --  '集团id'
,org_name                       --  '集团名称'
,area_id                       --  '区域id'
,area_name                       --  '区域名称'
,first_intention_type           --  '首次意向'
,intention_type           --  '当前意向'                        
,regist_time
,card_type
from
db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp t 
lateral view explode(split(card_types,',')) l1 as card_type
) b
) t1
group by t1.sub_org_id,t1.card_type,t1.regist_year) b1 
full join 
(
select 
aa2.sub_org_id
,card_type
,aa2.deposit_year
,count(distinct intention_customer_id) as deposit_cust_cnt  -- '下定客户数'
,sum(deposit_amount)    deposit_amount         --     '下定额'
,count(distinct membership_id)   as deposit_member_cnt
,count(distinct room_id)         as deposit_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,deposit_amount             --     '下定额'
,from_unixtime( cast(deposit_time as int),'yyyy')  as deposit_year 
,t.membership_id
,t.room_id
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa2
group by aa2.sub_org_id,aa2.card_type,aa2.deposit_year
)  tt2
on b1.sub_org_id=tt2.sub_org_id  and b1.card_type=tt2.card_type and b1.regist_year=tt2.deposit_year
full join 
(
select 
aa3.sub_org_id
,aa3.sign_year
,aa3.card_type
,count(distinct intention_customer_id) as sign_cust_cnt  -- '下定客户数'
,sum(receivable_amount) sales_amount            --  '销售金额'
,sum(received_amount)   received_amount            --  '已收金额'
,count(distinct membership_id)   as sign_member_cnt
,count(distinct room_id)         as sign_room_cnt
from 
(
select
 t.intention_customer_id
,sub_org_id 
,card_type
,t.membership_id
,t.room_id
,from_unixtime( cast(sign_time as int),'yyyy')  as sign_year
,receivable_amount             --  '应收金额'
,received_amount               --  '已收金额' 
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa3
group by aa3.sub_org_id,aa3.card_type,aa3.sign_year
)  tt3
on b1.sub_org_id=tt3.sub_org_id and b1.card_type=tt3.card_type and b1.regist_year=tt3.sign_year
full join 
(
select
aa7.sub_org_id
,aa7.card_type
,aa7.apply_year
,count(distinct pre_membership_id) as apply_member_cnt
from 
(
select 
t.pre_membership_id
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.apply_time as int),'yyyy')  apply_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa7
group by aa7.sub_org_id,aa7.card_type,aa7.apply_year
) tt7
on b1.sub_org_id=tt7.sub_org_id  and b1.card_type=tt7.card_type and b1.regist_year=tt7.apply_year
full join 
(
select
aa8.sub_org_id
,aa8.card_type
,aa8.transfer_to_year
,count(distinct membership_id) as transfer_to_member_cnt
,sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' 
,sum(transfor_to_amount)          transfor_to_amount      --  '转让费'
from 
(
select 
t.membership_id
,t.other_transfor_to_amount      --  '其他转让费' 
,t.transfor_to_amount            --  '转让费'
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_to_time as int),'yyyy')  transfer_to_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa8
group by aa8.sub_org_id,aa8.card_type,aa8.transfer_to_year
) tt8
on b1.sub_org_id=tt8.sub_org_id and b1.card_type=tt8.card_type and b1.regist_year=tt8.transfer_to_year
full join 
(
select
aa9.sub_org_id
,aa9.card_type
,aa9.transfer_from_year
,count(distinct membership_id) as transfer_from_member_cnt
,sum(other_transfer_from_amount)    other_transfer_from_amount     
,sum(transfer_from_amount)          transfer_from_amount   
from   
(
select 
t.membership_id
,t.other_transfor_from_amount as other_transfer_from_amount       
,t.transfor_from_amount  as transfer_from_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.transfor_from_time as int),'yyyy')  transfer_from_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa9
group by aa9.sub_org_id, aa9.card_type,aa9.transfer_from_year
) tt9
on b1.sub_org_id=tt9.sub_org_id and b1.card_type=tt9.card_type and b1.regist_year=tt9.transfer_from_year
full join 
(
select
aa10.sub_org_id
,aa10.card_type
,aa10.withdraw_year
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount     
from 
(
select 
t.membership_id      
,t.withdraw_amount            
,t.sub_org_id
,t.card_type
,from_unixtime( cast(t.withdraw_time as int),'yyyy')  withdraw_year
from 
db_dw_hr.clife_hr_dwd_market t 
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
)  aa10
group by aa10.sub_org_id,aa10.card_type,aa10.withdraw_year
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_year=tt10.withdraw_year;




insert overwrite table db_dw_hr.clife_hr_dws_market_by_card_type_y partition(part_date)
select
 a.card_type                           --   '会籍卡id'                             
,a.sub_org_id                          --   '养老机构id'
,b.sub_org_name                        --   '养老机构名称'
,b.org_id                              --   '养老集团id'
,b.org_name                            --   '养老集团名称'
,c.area_id                             --   '区域id'
,c.area_name                           --   '区域名称'
,a.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,a.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,a.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,a.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,a.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,a.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,a.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,a.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数''
,a.crm_intent_cnt                      --   '营销管理模块意向客户数'
,a.deposit_cust_cnt                    --   '下定客户数'
,a.sign_cust_cnt                       --   '签约客户数'
,a.transfer_to_member_cnt              --   '转让会籍数'
,a.transfer_from_member_cnt            --   '继承会籍数'
,a.withdraw_member_cnt                 --   '退会会籍数'
,a.apply_member_cnt                    --   '预申请会籍数量'
,a.deposit_member_cnt                  --   '下定会籍数量'
,a.deposit_room_cnt                    --   '下定房间数量'
,a.sign_member_cnt                     --   '签约会籍数量'
,a.sign_room_cnt                       --   '签约房间数量'
,a.deposit_amount                      --  '下定额'
,a.sales_amount                        --  '销售额'
,a.data_year                           --   '数据日期'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from 
db_dw_hr.clife_hr_dws_market_by_card_type_y_tmp a 
left join 
db_dw_hr.clife_hr_dim_admin_sub_org b
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','') left join db_dw_hr.clife_hr_dim_geographic_info c on concat(substr(b.city,1,5),b.area,'0000')=c.area_id  and c.part_date=regexp_replace(date_sub(current_date(),1),'-','');  





