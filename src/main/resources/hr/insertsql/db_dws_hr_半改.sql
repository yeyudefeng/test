===========================
    clife_hr_customer_crm_visit_base_info_tmp
===========================
create table if not exists db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp stored as parquet as
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
from db_dw_hr.clife_hr_dwd_market a 
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1
group by a.intention_customer_id


===========================
    drop_clife_hr_customer_crm_visit_base_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_customer_crm_visit_base_info_tmp


===========================
    clife_hr_dws_market_by_sub_org_f
===========================
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
,sum(assess_cnt) as assess_cnt   --'总评估次数'
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
on b1.sub_org_id=e1.sub_org_id


===========================
    clife_hr_dws_market_by_sub_org_d_tmp
===========================
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
(
select
a.sub_org_id
,a.regist_date
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
) a
group by a.sub_org_id,regist_date
)  a1
full join 
(select
a.sub_org_id
,a.adviser_date
,count(distinct advisory_customer_id)  as advisory_customer_cnt    --'咨询客户数',
from 
(select 
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
a.sub_org_id
,a.assess_date
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(
select 
t.intention_customer_id
,t.sub_org_id                         --  '养老机构id'
,assess_time2
,substr(assess_time2,1,10) as assess_date                         -- 列开的发起评估时间
from db_dw_hr.clife_hr_dwd_market t 
lateral view explode(split(assess_time,',')) l1 as assess_time2
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=0
)  a 
group by a.sub_org_id,a.assess_date
)  ta1
on a1.sub_org_id=ta1.sub_org_id  and a1.regist_date=ta1.assess_date
full join 
(
select
aa.sub_org_id
,aa.ask_date
,count(aa.intention_customer_id)   ask_check_in_cust_cnt            --'发起入住通知客户数'
from 
(
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
aa2.sub_org_id
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
aa6.sub_org_id
,aa6.assess_date
,count(distinct intention_customer_id)  as assess_cust_cnt  --'发起评估客户数'
,count(assess_time2)           as   assess_cnt              --'评估次数'
from 
(     --评估相关过程
select 
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
aa7.sub_org_id
,aa7.apply_date
,count(distinct pre_membership_id) as apply_member_cnt
from
(
select 
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
aa8.sub_org_id
,aa8.transfer_to_date
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
aa9.sub_org_id
,aa9.transfer_from_date
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
aa10.sub_org_id
,aa10.withdraw_date
,count(distinct membership_id) as withdraw_member_cnt   
,sum(withdraw_amount)          withdraw_amount 
from    
(
select 
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
on a1.sub_org_id=tt10.sub_org_id and a1.regist_date=tt10.withdraw_date


===========================
    clife_hr_dws_market_by_sub_org_d
===========================
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
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    drop_clife_hr_dws_market_by_sub_org_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_sub_org_d_tmp


===========================
    drop_clife_hr_dws_market_by_sub_org_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_sub_org_m_tmp


===========================
    clife_hr_dws_market_by_sub_org_m_tmp
===========================
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
on a1.sub_org_id=tt10.sub_org_id and a1.regist_month=tt10.withdraw_month


===========================
    clife_hr_dws_market_by_sub_org_m
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_sub_org_y_tmp
===========================
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
on a1.sub_org_id=tt10.sub_org_id and a1.regist_year=tt10.withdraw_year


===========================
    drop_clife_hr_dws_market_by_sub_org_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_sub_org_y_tmp


===========================
    clife_hr_dws_market_by_sub_org_y
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_channel_f
===========================
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
,count( distinct sign_cust)/count( distinct intention_customer_id) as sign_ratio                          --  '签约转化率'   -- 小姐姐用的,count( distinct sign_cust)/count( distinct deposit_cust_cnt)
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
lateral view outer explode(split(trade_channel_ids,',')) l1 as  channel_id   -- 增加了outer关键字
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null) -- 增加了null判断
)  a 
left join 
db_dw_hr.clife_hr_dim_trade_channel  b
on a.channel_id=b.id and  b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by a.sub_org_id,a.channel_id


===========================
    clife_hr_dws_market_by_channel_d_tmp
===========================
create table db_dw_hr.clife_hr_dws_market_by_channel_d_tmp stored as parquet as 
SELECT
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
a.sub_org_id
,a.channel_id
,a.apply_date  
,count(distinct pre_membership_id)     as apply_member_cnt 
from 
(
select 
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
intention_customer_id              --  '意向客户id'
,membership_id                      --  '会籍id'
,room_id
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
a.sub_org_id
,a.channel_id
,a.regist_date  
,count(distinct intention_customer_id)     as crm_intent_cnt 
from 
(
select 
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
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_date=tt4.regist_date


===========================
    drop_clife_hr_dws_market_by_channel_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_channel_d_tmp


===========================
    clife_hr_dws_market_by_channel_d
===========================
insert overwrite table db_dw_hr.clife_hr_dws_market_by_channel_d partition(part_date)
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
left join 
db_dw_hr.clife_hr_dim_geographic_info d 
on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_channel_m_tmp
===========================
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
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_month=tt4.regist_month


===========================
    drop_clife_hr_dws_market_by_channel_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_channel_m_tmp


===========================
    clife_hr_dws_market_by_channel_m
===========================
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
left join 
db_dw_hr.clife_hr_dim_geographic_info d 
on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_adv_member_d_tmp
===========================
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
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_date=tt7.apply_date


===========================
    drop_clife_hr_dws_market_by_adv_member_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_adv_member_d_tmp


===========================
    clife_hr_dws_market_by_adv_member_d
===========================
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
left join 
db_dw_hr.clife_hr_dim_geographic_info d 
on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_adv_member_m_tmp
===========================
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
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_month=tt7.apply_month


===========================
    drop_clife_hr_dws_market_by_adv_member_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_adv_member_m_tmp


===========================
    clife_hr_dws_market_by_adv_member_m
===========================
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
left join 
db_dw_hr.clife_hr_dim_geographic_info d 
on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_adv_member_y_tmp
===========================
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
on a1.sub_org_id=tt7.sub_org_id and a1.membership_adviser_id=tt7.membership_adviser_id and a1.regist_year=tt7.apply_year


===========================
    drop_clife_hr_dws_market_by_adv_member_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_adv_member_y_tmp


===========================
    clife_hr_dws_market_by_adv_member_y
===========================
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
left join 
db_dw_hr.clife_hr_dim_geographic_info d 
on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_crm_cust_building_info_tmp  -- online
===========================
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
group by t.intention_customer_id


===========================
    drop_clife_hr_dws_crm_cust_building_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp


===========================
    clife_hr_dws_crm_member_building_info_tmp  -- online
===========================
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
group by t.membership_id


===========================
    drop_clife_hr_dws_crm_member_building_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_member_building_info_tmp


===========================
    clife_hr_dws_market_by_building_f  -- online
===========================
/*
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
t1.sub_org_id
,t1.building_id
,sum(t1.deposit_room_cnt)  deposit_room_cnt
,sum(t1.sign_room_cnt)  sign_room_cnt
from 
(
select 
c.room_id
,c.sub_org_id
,c.building_id
,case when deposit_time is null then 1 else 0 end as deposit_room_cnt
,case when sign_time    is null then 1 else 0 end as sign_room_cnt
from 
(select 
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
on b1.sub_org_id=e1.sub_org_id  and b1.building_id=e1.building_id
*/
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
,e1.card_pay_amount                     --      '刷卡金额'
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
,case when b.deposit_time is not null and b.deposit_time != '' then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null and b.sign_time != '' then 1 else 0 end as sign_cust_cnt
,case when b.deposit_time is not null and b.deposit_time != '' then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_vist_cnt
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
,l1.building_id
from db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp t
lateral view outer explode(split(building_ids,','))   l1 as  building_id
) b
) t1
group by t1.sub_org_id,t1.building_id) b1
full join
(
select
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
c.membership_id
,c.sub_org_id
,c.building_id
,case when apply_time   is not null and apply_time != '' then 1 else 0 end as apply_member_cnt
,case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_member_cnt
,case when sign_time   is not null and sign_time != '' then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is not null and transfer_to_time != '' then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is not null and transfer_from_time != '' then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is not null and withdraw_time != '' then 1 else 0 end as withdraw_member_cnt
from
(select
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
lateral view outer explode(split(building_ids,','))   l1 as  building_id )  c
) t1
group by t1.sub_org_id,t1.building_id) c1
on b1.sub_org_id=c1.sub_org_id  and b1.building_id=c1.building_id
left join
(
select
t1.sub_org_id
,t1.building_id
,sum(t1.deposit_room_cnt)  deposit_room_cnt
,sum(t1.sign_room_cnt)  sign_room_cnt
from
(
select
c.room_id
,c.sub_org_id
,c.building_id
,case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_room_cnt
,case when sign_time   is not null and sign_time != '' then 1 else 0 end as sign_room_cnt
from
(select
 t.room_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,t.building_id  building_id
,concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'
,concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
group by t.room_id,t.building_id)  c
) t1
group by t1.sub_org_id,t1.building_id) d1
on b1.sub_org_id=d1.sub_org_id  and b1.building_id=d1.building_id
left join
(select
 sub_org_id
,building_id
,sum(deposit_amount)    deposit_amount         --     '下定额'
,sum(card_pay_amount)   card_pay_amount         --        '刷卡金额'
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
on b1.sub_org_id=e1.sub_org_id  and b1.building_id=e1.building_id



===========================
    clife_hr_customer_crm_visit_building_info_tmp -- online
===========================
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
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
group by a.intention_customer_id


===========================
    drop_clife_hr_customer_crm_visit_building_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp


===========================
    clife_hr_dws_market_by_building_d_tmp  -- online
===========================
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
,from_unixtime( cast(b.regist_time as int),'yyyy-MM-dd')  as regist_date
,l1.building_id
from 
db_dw_hr.clife_hr_dws_crm_cust_building_info_tmp b 
lateral view outer explode(split(building_ids,',')) l1 as building_id
) as t1
group by t1.sub_org_id,t1.building_id,t1.regist_date) b1 
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
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
,case when t.visit_time is not null and t.visit_time != '' and visit_time2 is not null and visit_time2 != '' and t.deposit_time_list is not null and t.deposit_time_list != '' then 1 else 0 end deposit_visit_cnt
,case when t.visit_time is not null and t.visit_time != '' and visit_time2 is not null and visit_time2 != '' and t.sign_time_list is not null and t.sign_time_list != '' then 1 else 0 end sign_visit_cnt
,case when t.visit_time is not null and t.visit_time != '' and visit_time2 is not null and visit_time2 != '' and t.deposit_time_list is not null and t.deposit_time_list != '' then intention_customer_id else null end deposit_visit_cust_cnt
,case when t.visit_time is not null and t.visit_time != '' and visit_time2 is not null and visit_time2 != '' and t.sign_time_list is not null and t.sign_time_list != '' then intention_customer_id else null end sign_visit_cust_cnt
from db_dw_hr.clife_hr_customer_crm_visit_building_info_tmp t 
lateral view outer explode(split(visit_time,',')) l1 as visit_time2
lateral view outer explode(split(building_ids,',')) l2 as building_id
)  aa5 
group by aa5.sub_org_id,aa5.building_id,aa5.visit_date
)  tt5
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
lateral view outer explode(split(assess_time,',')) l1 as assess_time2
lateral view outer explode(split(building_ids,',')) l2 as building_id
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
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
where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)
)  aa10
group by aa10.sub_org_id,aa10.building_id,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_date=tt10.withdraw_date


===========================
    drop_clife_hr_dws_market_by_building_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_building_d_tmp


===========================
    clife_hr_dws_market_by_building_d
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_building_m_tmp
===========================
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
lateral view outer explode(split(building_ids,',')) l1 as building_id
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
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_month=tt10.withdraw_month


===========================
    drop_clife_hr_dws_market_by_building_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_building_m_tmp


===========================
    clife_hr_dws_market_by_building_m
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_building_y_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_year=tt10.withdraw_year


===========================
    drop_clife_hr_dws_market_by_building_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_building_y_tmp


===========================
    clife_hr_dws_market_by_building_y
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_crm_cust_room_type_info_tmp
===========================
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
group by t.intention_customer_id


===========================
    drop_clife_hr_dws_crm_cust_room_type_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_cust_room_type_info_tmp


===========================
    clife_hr_dws_crm_member_room_type_info_tmp
===========================
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
group by t.membership_id


===========================
    drop_clife_hr_dws_crm_member_room_type_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_member_room_type_info_tmp


===========================
    clife_hr_dws_market_by_room_type_f
===========================
insert overwrite table db_dw_hr.clife_hr_dws_market_by_room_type_f partition(part_date)
select
 b1.room_type as room_type      --'房型'
,b1.sub_org_id as sub_org_id                          --   '养老机构id',
,b1.sub_org_name as sub_org_name                        --   '养老机构名称'
,b1.org_id as org_id                              --   '养老集团id'
,b1.org_name as org_name                            --   '养老集团名称'
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
,count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数'
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
-- ,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
-- ,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.deposit_time is not null and b.deposit_time != '' then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null and b.sign_time != '' then 1 else 0 end as sign_cust_cnt
,case when b.deposit_time is not null and b.deposit_time != '' then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_vist_cnt
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
lateral view outer explode(split(room_types,','))   l1 as  room_type    -- add outer
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
-- ,case when apply_time   is null then 1 else 0 end as apply_member_cnt
-- ,case when deposit_time is null then 1 else 0 end as deposit_member_cnt
-- ,case when sign_time   is null then 1 else 0 end as sign_member_cnt
-- ,case when transfer_to_time is null then 1 else 0 end as transfer_to_member_cnt
-- ,case when transfer_from_time is null then 1 else 0 end as transfer_from_member_cnt
-- ,case when withdraw_time is null then 1 else 0 end as withdraw_member_cnt
,case when apply_time   is not null and apply_time != '' then 1 else 0 end as apply_member_cnt
,case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_member_cnt
,case when sign_time   is not null and sign_time != '' then 1 else 0 end as sign_member_cnt
,case when transfer_to_time is not null and transfer_to_time != '' then 1 else 0 end as transfer_to_member_cnt
,case when transfer_from_time is not null and transfer_from_time != '' then 1 else 0 end as transfer_from_member_cnt
,case when withdraw_time is not null and withdraw_time != '' then 1 else 0 end as withdraw_member_cnt
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
lateral view outer explode(split(room_types,','))   l1 as  room_type )  c  -- add outer
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
-- ,case when deposit_time is null then 1 else 0 end as deposit_room_cnt
-- ,case when sign_time   is null then 1 else 0 end as sign_room_cnt
,case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_room_cnt
,case when sign_time   is not null and  sign_time != '' then 1 else 0 end as sign_room_cnt
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
on b1.sub_org_id=e1.sub_org_id  and b1.room_type=e1.room_type


===========================
    clife_hr_dws_market_by_room_type_d_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_date=tt10.withdraw_date


===========================
    drop_clife_hr_dws_market_by_room_type_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_room_type_d_tmp


===========================
    clife_hr_dws_market_by_room_type_d
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_room_type_m_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_month=tt10.withdraw_month


===========================
    drop_clife_hr_dws_market_by_room_type_m_tmp
===========================
drop table db_dw_hr.clife_hr_dws_market_by_room_type_m_tmp


===========================
    clife_hr_dws_market_by_room_type_m
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_room_type_y_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.room_type=tt10.room_type and b1.regist_year=tt10.withdraw_year


===========================
    drop_clife_hr_dws_market_by_room_type_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_room_type_y_tmp


===========================
    clife_hr_dws_market_by_room_type_y
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id 
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_crm_cust_card_info_tmp
===========================
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
group by t.intention_customer_id


===========================
    drop_clife_hr_dws_crm_cust_card_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_cust_card_info_tmp


===========================
    clife_hr_dws_crm_member_card_info_tmp
===========================
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
group by t.membership_id


===========================
    drop_clife_hr_dws_crm_member_card_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_member_card_info_tmp


===========================
    clife_hr_dws_crm_room_card_info_tmp
===========================
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
group by t.room_id


===========================
    drop_clife_hr_dws_crm_room_card_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_room_card_info_tmp


===========================
    clife_hr_dws_market_by_card_f
===========================
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
on b1.sub_org_id=e1.sub_org_id  and b1.card_id=e1.card_id


===========================
    clife_hr_dws_market_by_card_d_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_date=tt10.withdraw_date


===========================
    drop_clife_hr_dws_market_by_card_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_d_tmp


===========================
    clife_hr_dws_market_by_card_d
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_card_m_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_month=tt10.withdraw_month


===========================
    drop_clife_hr_dws_market_by_card_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_m_tmp


===========================
    clife_hr_dws_market_by_card_m
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_card_y_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_id=tt10.card_id and b1.regist_year=tt10.withdraw_year


===========================
    drop_clife_hr_dws_market_by_card_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_y_tmp


===========================
    clife_hr_dws_market_by_card_y
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_crm_cust_card_type_info_tmp
===========================
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
group by t.intention_customer_id


===========================
    drop_clife_hr_dws_crm_cust_card_type_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_cust_card_type_info_tmp


===========================
    clife_hr_dws_crm_member_card_type_info_tmp
===========================
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
group by t.membership_id


===========================
    drop_clife_hr_dws_crm_member_card_type_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_member_card_type_info_tmp


===========================
    clife_hr_dws_crm_room_card_type_info_tmp
===========================
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
group by t.room_id


===========================
    drop_clife_hr_dws_crm_room_card_type_info_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_crm_room_card_type_info_tmp


===========================
    clife_hr_dws_market_by_card_type_f
===========================
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
-- ,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt
-- ,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt
,case when b.deposit_time is not null  and b.deposit_time != '' then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null and b.sign_time != '' then 1 else 0 end as sign_cust_cnt
,case when b.deposit_time is not null and b.deposit_time != '' then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_vist_cnt
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
(
  select 
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
on b1.sub_org_id=e1.sub_org_id  and b1.card_type=e1.card_type


===========================
    clife_hr_dws_market_by_card_type_d_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_date=tt10.withdraw_date


===========================
    drop_clife_hr_dws_market_by_card_type_d_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_type_d_tmp


===========================
    clife_hr_dws_market_by_card_type_d
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_card_type_m_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_month=tt10.withdraw_month


===========================
    drop_clife_hr_dws_market_by_card_type_m_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_type_m_tmp


===========================
    clife_hr_dws_market_by_card_type_m
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_card_type_y_tmp
===========================
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
on b1.sub_org_id=tt10.sub_org_id and b1.card_type=tt10.card_type and b1.regist_year=tt10.withdraw_year


===========================
    drop_clife_hr_dws_market_by_card_type_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_card_type_y_tmp


===========================
    clife_hr_dws_market_by_card_type_y
===========================
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
on a.sub_org_id=b.sub_org_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
db_dw_hr.clife_hr_dim_geographic_info c 
on concat(substr(b.city,1,5),b.area,'0000')=c.area_id
and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_market_by_channel_y_tmp
===========================
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
on tt1.sub_org_id=tt4.sub_org_id and tt1.channel_id=tt4.channel_id and tt1.apply_year=tt4.regist_year


===========================
    drop_clife_hr_dws_market_by_channel_y_tmp
===========================
drop table if exists db_dw_hr.clife_hr_dws_market_by_channel_y_tmp


===========================
    clife_hr_dws_market_by_adv_member_f    --------  改
===========================
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
-- ,case when b.deposit_time is not null then 1 else 0 end as deposit_cust_cnt                  -- 小姐姐制造的bug
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_cnt              -- 小姐姐制造的bug * 2
-- ,case when b.deposit_time is not null then b.visit_cnt else 0 end as deposit_cust_visit_cnt  -- 小姐姐制造的bug
-- ,case when b.sign_time is not null then b.visit_cnt else 0 end as sign_cust_vist_cnt         -- 小姐姐制造的bug
,case when b.deposit_time is not null  and b.deposit_time != '' then 1 else 0 end as deposit_cust_cnt
,case when b.sign_time is not null and b.sign_time != '' then 1 else 0 end as sign_cust_cnt
,case when b.deposit_time is not null and b.deposit_time != '' then b.visit_cnt else 0 end as deposit_cust_visit_cnt
,case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_vist_cnt
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
on b1.sub_org_id=e1.sub_org_id  and b1.membership_adviser_id=e1.membership_adviser_id


===========================
    clife_hr_dws_market_by_channel_y
===========================
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
left join db_dw_hr.clife_hr_dim_geographic_info d on concat(substr(b.city,1,5),b.area,'0000')=d.area_id
and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')


===========================
    clife_hr_dws_reside_manage_by_soday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_soday partition(part_date)
select
     k.sub_org_id          --机构id
    ,k.sub_org_name        --机构名称
    ,k.org_id              --集团id
    ,k.org_name            --集团名称
    ,k.area_id             --区域id
    ,k.reside_cnt_day       --每天入住人数
    ,k.countermand_cnt_day  --每日退住人数
    ,k.room_num_day         --每日入住房间数
    ,k.bed_num_day          --每日入住床位数
    ,k.all_num              --每日总人数
    ,k.visit_num_day        --每日探访人数
    ,k.visit_time_day       --每日探访时长
    ,k.leave_num_day        --每日请假次数
    ,k.leave_time_day       --每日请假时长
    ,k.sick_leave_num_day   --每日病假次数
    ,k.casual_leave_num_day --每日事假次数
    ,k.other_leave_num_day  --每日其他假次数
    ,h.room_amount --楼层房间数
    ,h.bed_amount  --楼层床位数
    ,coalesce(k.reside_cnt_day/k.all_num,0)      as check_in_rate_day       --每日入住率
    ,coalesce(k.countermand_cnt_day/k.all_num,0) as countermand_rate_day    --每日退住率
    ,coalesce(k.room_num_day/h.room_amount,0)    as check_in_room_rate_day  --每日房间入住率
    ,coalesce((h.room_amount-k.room_num_day)/h.room_amount,0) as vacant_room_rate_day  --房间空房率
    ,coalesce(k.bed_num_day/h.bed_amount,0)                   as check_in_bed_rate_day --床位入住率
    ,coalesce((h.bed_amount-k.bed_num_day)/h.bed_amount,0)    as vacant_bed_rate_day   --床位空床率	
    ,k.contract_time_1_num_day        --录入合同数量（1年期）
    ,k.contract_time_2_num_day        --录入合同数量（2年期）
    ,k.contract_time_3_num_day        --录入合同数量（3年期）
    ,k.contract_time_4_num_day        --录入合同数量（4年期）
    ,k.contract_time_5_num_day        --录入合同数量（5年期）
    ,k.contract_time_surpass_5_num_day --录入合同数量（5年以上）
    ,k.refund_amount        --当天请假退费金额
    ,k.leave_reducation_fee --当天请假减免费用
    ,k.date_time --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)     as reside_cnt_day
      ,sum(countermand_cnt_day) as countermand_cnt_day
      ,sum(room_num_day)        as room_num_day
      ,sum(bed_num_day)         as bed_num_day
      ,sum(all_num)               as all_num
      ,sum(visit_num_day)         as visit_num_day       
      ,sum(visit_time_day)        as visit_time_day      
      ,sum(leave_num_day)         as leave_num_day       
      ,sum(leave_time_day)        as leave_time_day      
      ,sum(sick_leave_num_day)    as sick_leave_num_day  
      ,sum(casual_leave_num_day)  as casual_leave_num_day
      ,sum(other_leave_num_day)   as other_leave_num_day 
      ,sum(refund_amount)         as refund_amount
      ,sum(leave_reducation_fee)  as leave_reducation_fee
      ,sum(contract_time_1_num_day)        as contract_time_1_num_day        --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)        as contract_time_2_num_day        --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)        as contract_time_3_num_day        --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)        as contract_time_4_num_day        --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)        as contract_time_5_num_day        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day)as contract_time_surpass_5_num_day--录入合同数量（5年以上）
      ,date_time
  from db_dw_hr.clife_hr_dws_reside_manage_by_nhday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by sub_org_id
          ,date_time
)k
left join
(
   select
        b.sub_org_id
        ,count(a.room_id)  as room_amount 
        ,sum(a.bed_amount) as bed_amount
   from db_dw_hr.clife_hr_dim_location_room a
   left join db_dw_hr.clife_hr_dim_nursing_home b on a.nursing_homes_id=b.nursing_homes_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
   where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by b.sub_org_id
)h on k.sub_org_id=h.sub_org_id


===========================
    clife_hr_dws_reside_manage_by_fday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_fday partition(part_date)
select
     a.floor_id            --楼层id
    ,a.building_id         --楼栋id
    ,a.nursin_homes_id     --养老院id
    ,a.nursing_homes_name  --养老院名称
    ,a.sub_org_id          --机构id
    ,a.sub_org_name        --机构名称
    ,a.org_id              --集团id
    ,a.org_name            --集团名称
    ,a.area_id             --区域id
    ,a.reside_cnt_day       --每天入住人数
    ,a.countermand_cnt_day  --每日退住人数
    ,a.room_num_day         --每日入住房间数
    ,a.bed_num_day          --每日入住床位数
    ,a.all_num              --每日总人数
    ,a.visit_num_day        --每日探访人数
    ,a.visit_time_day       --每日探访时长
    ,a.leave_num_day        --每日请假次数
    ,a.leave_time_day       --每日请假时长
    ,a.sick_leave_num_day   --每日病假次数
    ,a.casual_leave_num_day --每日事假次数
    ,a.other_leave_num_day  --每日其他假次数
    ,b.room_amount --楼层房间数
    ,b.bed_amount  --楼层床位数
    ,coalesce(a.reside_cnt_day/a.all_num,0)      as check_in_rate_day       --每日入住率
    ,coalesce(a.countermand_cnt_day/a.all_num,0) as countermand_rate_day    --每日退住率
    ,coalesce(a.room_num_day/b.room_amount,0)    as check_in_room_rate_day  --每日房间入住率
    ,coalesce((b.room_amount-a.room_num_day)/b.room_amount,0) as vacant_room_rate_day  --房间空房率
    ,coalesce(a.bed_num_day/b.bed_amount,0)                   as check_in_bed_rate_day --床位入住率
    ,coalesce((b.bed_amount-a.bed_num_day)/b.bed_amount,0)    as vacant_bed_rate_day   --床位空床率	
    ,0 as contract_time_1_num_day        --录入合同数量（1年期）
    ,0 as contract_time_2_num_day        --录入合同数量（2年期）
    ,0 as contract_time_3_num_day        --录入合同数量（3年期）
    ,0 as contract_time_4_num_day        --录入合同数量（4年期）
    ,0 as contract_time_5_num_day        --录入合同数量（5年期）
    ,0 as contract_time_surpass_5_num_day --录入合同数量（5年以上）
    ,a.refund_amount        --当天请假退费金额
    ,a.leave_reducation_fee --当天请假减免费用
    ,a.date_time --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       floor_id            --楼层id
      ,max(building_id) as     building_id      --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)     as reside_cnt_day
      ,sum(countermand_cnt_day) as countermand_cnt_day
      ,sum(room_num_day)        as room_num_day
      ,sum(bed_num_day)         as bed_num_day
      ,sum(all_num)               as all_num
      ,sum(visit_num_day)         as visit_num_day       
      ,sum(visit_time_day)        as visit_time_day      
      ,sum(leave_num_day)         as leave_num_day       
      ,sum(leave_time_day)        as leave_time_day      
      ,sum(sick_leave_num_day)    as sick_leave_num_day  
      ,sum(casual_leave_num_day)  as casual_leave_num_day
      ,sum(other_leave_num_day)   as other_leave_num_day 
      ,sum(refund_amount)         as refund_amount
      ,sum(leave_reducation_fee)  as leave_reducation_fee
      ,date_time
  from
  (
    select
         b.floor_id            --楼层id
        ,max(b.building_id) as     building_id      --楼栋id
        ,max(a.nursin_homes_id)    as nursin_homes_id     --养老院id
        ,max(a.nursing_homes_name) as nursing_homes_name  --养老院名称
        ,max(a.sub_org_id)         as sub_org_id          --机构id
        ,max(a.sub_org_name)       as sub_org_name        --机构名称
        ,max(a.org_id)             as org_id              --集团id
        ,max(a.org_name)           as org_name            --集团名称
        ,max(a.area_id)            as area_id             --区域id 
        ,sum(case when a.reside_status = 3 then 1 else 0 end) as reside_cnt_day
        ,sum(case when a.reside_status = 4 then 1 else 0 end) as countermand_cnt_day
        ,count(case when a.reside_status = 3 then a.room_id else null end) as room_num_day
        ,count(case when a.reside_status = 3 then a.bed_id else null end) as bed_num_day
        ,0 as all_num
        ,0 as visit_num_day  --每天探访人数
        ,0 as visit_time_day --探访时长
        ,0 as leave_num_day        --请假次数
        ,0 as leave_time_day       --请假时长
        ,0 as sick_leave_num_day   --病假次数
        ,0 as casual_leave_num_day--事假次数
        ,0 as other_leave_num_day  --其他假次数
        ,0 as refund_amount
        ,0 as leave_reducation_fee
        ,substr(a.reside_time,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join  db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by b.floor_id
            ,substr(a.reside_time,1,10)
    union all
    select
         b.floor_id            --楼层id
        ,max(b.building_id) as     building_id      --楼栋id
        ,max(a.nursin_homes_id)    as nursin_homes_id     --养老院id
        ,max(a.nursing_homes_name) as nursing_homes_name  --养老院名称
        ,max(a.sub_org_id)         as sub_org_id          --机构id
        ,max(a.sub_org_name)       as sub_org_name        --机构名称
        ,max(a.org_id)             as org_id              --集团id
        ,max(a.org_name)           as org_name            --集团名称
        ,max(a.area_id)            as area_id             --区域id
        ,0 as reside_cnt_day
        ,0 as countermand_cnt_day
        ,0 as room_num_day
        ,0 as bed_num_day	
        ,count(1) as all_num
        ,0 as visit_num_day  --每天探访人数
        ,0 as visit_time_day --探访时长
        ,0 as leave_num_day        --请假次数
        ,0 as leave_time_day       --请假时长
        ,0 as sick_leave_num_day   --病假次数
        ,0 as casual_leave_num_day--事假次数
        ,0 as other_leave_num_day  --其他假次数
        ,0 as refund_amount
        ,0 as leave_reducation_fee
        ,substr(a.contract_entry_time,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_reside_manage_info a
    left join  db_dw_hr.clife_hr_dim_location_room b on a.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by b.floor_id
            ,substr(a.contract_entry_time,1,10)
    union all
    select
         b.floor_id            --楼层id
        ,max(b.building_id) as     building_id      --楼栋id
        ,max(t.nursin_homes_id)    as nursin_homes_id     --养老院id
        ,max(t.nursing_homes_name) as nursing_homes_name  --养老院名称
        ,max(t.sub_org_id)         as sub_org_id          --机构id
        ,max(t.sub_org_name)       as sub_org_name        --机构名称
        ,max(t.org_id)             as org_id              --集团id
        ,max(t.org_name)           as org_name            --集团名称
        ,max(t.area_id)            as area_id             --区域id
        ,0 as reside_cnt_day
        ,0 as countermand_cnt_day
        ,0 as room_num_day
        ,0 as bed_num_day
        ,0 as all_num
        ,count(t.relatives_name) as visit_num_day  --每天探访人数
        ,sum(CAST((if(t.visit_end_time is null,unix_timestamp(),unix_timestamp(t.visit_end_time)) - unix_timestamp(t.visit_start_time)) / 60 AS int) % 60) as visit_time_day --探访时长
        ,0 as leave_num_day        --请假次数
        ,0 as leave_time_day       --请假时长
        ,0 as sick_leave_num_day   --病假次数
        ,0 as casual_leave_num_day--事假次数
        ,0 as other_leave_num_day  --其他假次数
        ,0 as refund_amount
        ,0 as leave_reducation_fee
    	,substr(t.visit_start_time,1,10) as date_time --每天探访日期
    from
    (
     select
          a.old_man_id
         ,a.room_id
         ,a.nursin_homes_id   
         ,a.nursing_homes_name
         ,a.sub_org_id        
         ,a.sub_org_name      
         ,a.org_id            
         ,a.org_name          
         ,a.area_id           
         ,ip.relatives_name
         ,split(ip.visit_time,'-')[0] as visit_start_time
         ,split(ip.visit_time,'-')[1] as visit_end_time
     from db_dw_hr.clife_hr_dwd_reside_manage_info a
     lateral view explodemulti(if(a.relatives_names is null,'',a.relatives_names),if(a.visit_times is null,'',a.visit_times))ip as relatives_name,visit_time
     where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    left join  db_dw_hr.clife_hr_dim_location_room b on t.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by b.floor_id
            ,substr(t.visit_start_time,1,10)
    union all
    select
         b.floor_id            --楼层id
        ,max(b.building_id) as     building_id      --楼栋id
        ,max(t.nursin_homes_id)    as nursin_homes_id     --养老院id
        ,max(t.nursing_homes_name) as nursing_homes_name  --养老院名称
        ,max(t.sub_org_id)         as sub_org_id          --机构id
        ,max(t.sub_org_name)       as sub_org_name        --机构名称
        ,max(t.org_id)             as org_id              --集团id
        ,max(t.org_name)           as org_name            --集团名称
        ,max(t.area_id)            as area_id             --区域id
        ,0 as reside_cnt_day
        ,0 as countermand_cnt_day
        ,0 as room_num_day
        ,0 as bed_num_day
        ,0 as all_num
        ,0 as visit_num_day  --每天探访人数
        ,0 as visit_time_day --探访时长
        ,count(1)    as leave_num_day        --请假次数
        ,sum(t.leave_time) as leave_time_day       --请假时长
        ,sum(case when t.leave_type = 0 then 1 else 0 end ) as sick_leave_num_day   --病假次数
        ,sum(case when t.leave_type = 1 then 1 else 0 end ) as casual_leave_num_day--事假次数
        ,sum(case when t.leave_type = 2 then 1 else 0 end ) as other_leave_num_day  --其他假次数
        ,0 as refund_amount
        ,0 as leave_reducation_fee
        ,substr(t.leave_create_time,1,10) as date_time
    from
    (
     select
           a.old_man_check_in_id
          ,a.room_id
          ,a.old_man_id
          ,a.nursin_homes_id   
          ,a.nursing_homes_name
          ,a.sub_org_id        
          ,a.sub_org_name      
          ,a.org_id            
          ,a.org_name          
          ,a.area_id  
          ,ip.leave_create_time
          ,ip.leave_time
          ,ip.leave_type
      from db_dw_hr.clife_hr_dwd_reside_manage_info a
      lateral view explodemulti(if(a.leave_create_times is null,'',a.leave_create_times),if(a.leave_times is null,'',a.leave_times),if(a.leave_types is null,'',a.leave_types))ip as leave_create_time,leave_time,leave_type
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    left join  db_dw_hr.clife_hr_dim_location_room b on t.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by b.floor_id
            ,substr(t.leave_create_time,1,10)
    union all
    select
         b.floor_id            --楼层id
        ,max(b.building_id) as     building_id      --楼栋id
        ,max(t1.nursin_homes_id)    as nursin_homes_id     --养老院id
        ,max(t1.nursing_homes_name) as nursing_homes_name  --养老院名称
        ,max(t1.sub_org_id)         as sub_org_id          --机构id
        ,max(t1.sub_org_name)       as sub_org_name        --机构名称
        ,max(t1.org_id)             as org_id              --集团id
        ,max(t1.org_name)           as org_name            --集团名称
        ,max(t1.area_id)            as area_id             --区域id
        ,0 as reside_cnt_day
        ,0 as countermand_cnt_day
        ,0 as room_num_day
        ,0 as bed_num_day
        ,0 as all_num
        ,0 as visit_num_day  --每天探访人数
        ,0 as visit_time_day --探访时长
        ,0 as leave_num_day        --请假次数
        ,0 as leave_time_day       --请假时长
        ,0 as sick_leave_num_day   --病假次数
        ,0 as casual_leave_num_day--事假次数
        ,0 as other_leave_num_day  --其他假次数
        ,sum(t1.refund_amount) as refund_amount
        ,sum(t1.leave_reducation_fee) as leave_reducation_fee
        ,substr(t1.leave_create_time,1,10) as leave_time
    from
    (
      select
       t.*
      ,row_number()over(partition by old_man_check_in_id order by t.leave_create_time desc) as rank
      from
      (
        select
             a.old_man_check_in_id
            ,a.room_id
            ,a.old_man_id
            ,a.nursin_homes_id   
            ,a.nursing_homes_name
            ,a.sub_org_id        
            ,a.sub_org_name      
            ,a.org_id            
            ,a.org_name          
            ,a.area_id  
            ,a.refund_amount                      --当天请假退费金额
            ,a.leave_reducation_fee               --当天请假减免费用
            ,ip.leave_create_time
            ,ip.leave_time
            ,ip.leave_type
        from db_dw_hr.clife_hr_dwd_reside_manage_info a
        lateral view explodemulti(if(a.leave_create_times is null,'',a.leave_create_times),if(a.leave_times is null,'',a.leave_times),if(a.leave_types is null,'',a.leave_types))ip as leave_create_time,leave_time,leave_type
        where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
      )t
    )t1
    left join  db_dw_hr.clife_hr_dim_location_room b on t1.room_id = b.room_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    where t1.rank = 1
    group by b.floor_id
            ,substr(t1.leave_create_time,1,10)
  )a
  group by floor_id
          ,date_time
)a
left join
(
  select
       floor_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by floor_id
)b on a.floor_id=b.floor_id


===========================
    clife_hr_dws_activity_manage_by_soday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_soday partition(part_date)
select
     sub_org_id
    ,activity_type
    ,sub_org_name             --机构名称
    ,org_id                  --集团id
    ,org_name                --集团名称
    ,area_id                 --区域id
    ,reserve_cnt_day           --预定活动室次数
    ,template_cnt_day           --使用活动模板数量
    ,activity_cnt_day          --已创建活动数
    ,activity_fee_day          --活动费用
    ,host_activity_cnt_day     --已举办活动数
    ,confirm_old_man_cnt_day   --到场人次
    ,sign_old_man_cnt_day      --签到人次
    ,apply_old_man_cnt_day     --报名人次
    ,apply_activity_cnt_day    --已报名活动数
    ,date_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       sub_org_id
      ,activity_type
      ,max(sub_org_name)      as sub_org_name             --机构名称
      ,max(org_id)             as org_id                  --集团id
      ,max(org_name)           as org_name                --集团名称
      ,max(area_id)            as area_id                 --区域id
      ,sum(reserve_cnt_day)          as reserve_cnt_day           --预定活动室次数
      ,sum(template_cnt_day)          as template_cnt_day           --使用活动模板数量
      ,sum(activity_cnt_day)         as activity_cnt_day          --已创建活动数
      ,sum(activity_fee_day)         as activity_fee_day          --活动费用
      ,sum(host_activity_cnt_day)    as host_activity_cnt_day     --已举办活动数
      ,sum(confirm_old_man_cnt_day)  as confirm_old_man_cnt_day   --到场人次
      ,sum(sign_old_man_cnt_day)     as sign_old_man_cnt_day      --签到人次
      ,sum(apply_old_man_cnt_day)    as apply_old_man_cnt_day     --报名人次
      ,sum(apply_activity_cnt_day)   as apply_activity_cnt_day    --已报名活动数
      ,date_time
  from
  (
    select
         sub_org_id
        ,activity_type
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,count(t.reserve_id) as reserve_cnt_day  --预定活动室次数
        ,0 as template_cnt_day   --使用活动模板数量
        ,0 as activity_cnt_day  --已创建活动数
        ,0 as activity_fee_day   --活动费用
        ,0 as host_activity_cnt_day  --已举办活动数
        ,0 as confirm_old_man_cnt_day       --到场人次
        ,0 as sign_old_man_cnt_day       --签到人次
        ,0 as apply_old_man_cnt_day       --报名人次
        ,0 as apply_activity_cnt_day       --已报名活动数
        ,substr(t.reserve_time,1,10) as date_time
    from
    (
      select
           a.activity_type
          ,a.sub_org_id           --机构id
          ,a.sub_org_name         --机构名称
          ,a.org_id               --集团id
          ,a.org_name             --集团名称
          ,a.area_id              --区域id
          ,ip.reserve_id
          ,ip.reserve_time
      from db_dw_hr.clife_hr_dwd_activity_manage_info a
      lateral view explodemulti(if(a.reserve_ids is null,'',a.reserve_ids),if(a.reserve_times is null,'',a.reserve_times))ip as reserve_id,reserve_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    group by activity_type
            ,sub_org_id
            ,substr(t.reserve_time,1,10)
    union all
    select         
         sub_org_id
        ,activity_type				
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,0 as reserve_cnt_day  --预定活动室次数
        ,count(template_id) as template_cnt_day   --使用活动模板数量
        ,count(activity_id) as activity_cnt_day  --已创建活动数
        ,sum(activity_fee)  as activity_fee_day   --活动费用
        ,0 as host_activity_cnt_day  --已举办活动数
        ,0 as confirm_old_man_cnt_day       --到场人次
        ,0 as sign_old_man_cnt_day       --签到人次
        ,0 as apply_old_man_cnt_day       --报名人次
        ,0 as apply_activity_cnt_day       --已报名活动数
        ,substr(activity_create_time,1,10) as date_time --活动创建时间
    from db_dw_hr.clife_hr_dwd_activity_manage_info
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by activity_type	
            ,sub_org_id	
            ,substr(activity_create_time,1,10)
    union all
    select
         sub_org_id
        ,activity_type	 
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,0 as reserve_cnt_day  --预定活动室次数
        ,0 as template_cnt_day   --使用活动模板数量
        ,0 as activity_cnt_day  --已创建活动数
        ,0 as activity_fee_day   --活动费用
        ,count(activity_id) as host_activity_cnt_day  --已举办活动数
        ,0 as confirm_old_man_cnt_day       --到场人次
        ,0 as sign_old_man_cnt_day       --签到人次
        ,0 as apply_old_man_cnt_day       --报名人次
        ,0 as apply_activity_cnt_day       --已报名活动数
        ,substr(activity_time,1,10) as date_time --活动创建时间
    from db_dw_hr.clife_hr_dwd_activity_manage_info
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by activity_type
            ,sub_org_id	
            ,substr(activity_time,1,10)
    union all
    select
         sub_org_id
        ,activity_type	
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,0 as reserve_cnt_day  --预定活动室次数
        ,0 as template_cnt_day   --使用活动模板数量
        ,0 as activity_cnt_day  --已创建活动数
        ,0 as activity_fee_day   --活动费用
        ,0 as host_activity_cnt_day  --已举办活动数
        ,count(t.confirm_old_man)  as confirm_old_man_cnt_day       --到场人次
        ,0 as sign_old_man_cnt_day       --签到人次
        ,0 as apply_old_man_cnt_day       --报名人次
        ,0 as apply_activity_cnt_day       --已报名活动数
        ,substr(t.confirm_time,1,10) as date_time
    from
    (
      select
           a.activity_id
          ,a.activity_type
          ,a.sub_org_id             --机构id
          ,a.sub_org_name           --机构名称
          ,a.org_id                --集团id
          ,a.org_name              --集团名称
          ,a.area_id               --区域id
          ,ip.confirm_old_man
          ,ip.confirm_time
      from db_dw_hr.clife_hr_dwd_activity_manage_info a
      lateral view explodemulti(if(a.confirm_old_mans is null,'',a.confirm_old_mans),if(a.confirm_times is null,'',a.confirm_times))ip as confirm_old_man,confirm_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    group by activity_type
            ,sub_org_id	
            ,substr(t.confirm_time,1,10)
    union all
    select
         activity_type
        ,sub_org_id
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,0 as reserve_cnt_day  --预定活动室次数
        ,0 as template_cnt_day   --使用活动模板数量
        ,0 as activity_cnt_day  --已创建活动数
        ,0 as activity_fee_day   --活动费用
        ,0 as host_activity_cnt_day  --已举办活动数
        ,0 as confirm_old_man_cnt_day       --到场人次
        ,count(t.sign_old_man)       as sign_old_man_cnt_day       --签到人次
        ,0 as apply_old_man_cnt_day       --报名人次
        ,0 as apply_activity_cnt_day       --已报名活动数
        ,substr(t.sign_time,1,10) as date_time
    from
    (
      select
           a.activity_id
          ,a.activity_type
          ,a.nurse_home_id
          ,a.nursing_homes_name   --养老院名称
          ,a.sub_org_id           --机构id
          ,a.sub_org_name         --机构名称
          ,a.org_id               --集团id
          ,a.org_name             --集团名称
          ,a.area_id              --区域id
          ,ip.sign_old_man
          ,ip.sign_time
      from db_dw_hr.clife_hr_dwd_activity_manage_info a
      lateral view explodemulti(if(a.sign_old_mans is null,'',a.sign_old_mans),if(a.sign_times is null,'',a.sign_times))ip as sign_old_man,sign_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    group by activity_type
            ,sub_org_id
            ,substr(t.sign_time,1,10)
    union all
    select
         activity_type
        ,sub_org_id
        ,max(sub_org_name)      as sub_org_name           --机构名称
        ,max(org_id)             as org_id                  --集团id
        ,max(org_name)           as org_name                --集团名称
        ,max(area_id)            as area_id                 --区域id
        ,0 as reserve_cnt_day  --预定活动室次数
        ,0 as template_cnt_day   --使用活动模板数量
        ,0 as activity_cnt_day  --已创建活动数
        ,0 as activity_fee_day   --活动费用
        ,0 as host_activity_cnt_day  --已举办活动数
        ,0 as confirm_old_man_cnt_day       --到场人次
        ,0 as sign_old_man_cnt_day       --签到人次
        ,count(t.apply_time)       as apply_old_man_cnt_day       --报名人次
        ,count(t.apply_time)       as apply_activity_cnt_day       --已报名活动数
        ,substr(t.apply_time,1,10) as date_time
    from
    (
      select
           a.activity_id
          ,a.activity_type
          ,a.nurse_home_id
          ,a.nursing_homes_name   --养老院名称
          ,a.sub_org_id           --机构id
          ,a.sub_org_name         --机构名称
          ,a.org_id               --集团id
          ,a.org_name             --集团名称
          ,a.area_id              --区域id
          ,ip.apply_time
      from db_dw_hr.clife_hr_dwd_activity_manage_info a
      lateral view explodemulti(if(a.apply_times is null,'',a.apply_times)) ip as apply_time
      where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
    )t
    group by activity_type
            ,sub_org_id
            ,substr(t.apply_time,1,10)
  )k
  group by  sub_org_id
            ,activity_type
            ,date_time
)h


===========================
    clife_hr_dws_plan_snapshot_by_protypeyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_protypeyear partition(part_date)
select
     sub_org_id         --机构id
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_year  --计划总数
    ,item_num_year  --任务总数
    ,complete_num_year --完成任务数
    ,coalesce(complete_num_year/item_num_year,0) as complete_rate_year --完成率
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       sub_org_id        --机构id
      ,project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_month) as plan_num_year  --计划总数
      ,sum(item_num_month) as item_num_year  --任务总数
      ,sum(complete_num_month) as complete_num_year --完成任务数
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_plan_snapshot_by_protypemonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by project_type_id
           ,sub_org_id
           ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_plan_snapshot_by_protypeday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_protypeday partition(part_date)
select
     sub_org_id         --机构id
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_day  --计划总数
    ,item_num_day  --任务总数
    ,complete_num_day --完成任务数
    ,coalesce(complete_num_day/item_num_day,0) as complete_rate_day --完成率
    ,date_time  --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       sub_org_id        --机构id
      ,project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_day) as plan_num_day  --计划总数
      ,sum(item_num_day) as item_num_day  --任务总数
      ,sum(complete_num_day) as complete_num_day --完成任务数
      ,date_time
  from
  (
    select
         sub_org_id        --机构id
        ,project_type_id    --项目类型id
        ,max(project_type_name)as project_type_name  --项目类型名称
        ,max(sub_org_name)     as sub_org_name       --机构名称
        ,max(org_id)           as org_id             --集团id
        ,max(org_name)         as org_name           --集团名称
        ,0 as plan_num_day  --计划总数
        ,count(1) as  item_num_day  --任务总数
        ,sum(case when complete_stase = 1 then 1 else 0 end) as complete_num_day --完成任务数
        ,substr(plan_create_time,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_plan_snapshot_info
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by project_type_id
            ,sub_org_id
            ,substr(plan_create_time,1,10)
    union all
    select
         sub_org_id        --机构id
        ,project_type_id    --项目类型id
        ,max(project_type_name)as project_type_name  --项目类型名称
        ,max(sub_org_name)     as sub_org_name       --机构名称
        ,max(org_id)           as org_id             --集团id
        ,max(org_name)         as org_name           --集团名称
        ,count(exec_date) as plan_num_day  --计划总数
        ,0 as  item_num_day  --任务总数
        ,0 as complete_num_day --完成任务数
        ,substr(exec_date,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_plan_snapshot_info
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by project_type_id
            ,sub_org_id
             ,substr(exec_date,1,10)
  )a
  group by project_type_id
          ,sub_org_id
          ,date_time
)t


===========================
    clife_hr_dws_plan_snapshot_by_proyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_proyear partition(part_date)
select
     project_id         --项目id
    ,sub_org_id         --机构id
    ,project_name       --项目名称
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_year  --计划总数
    ,item_num_year  --任务总数
    ,complete_num_year --完成任务数
    ,coalesce(complete_num_year/item_num_year,0) as complete_rate_year --完成率
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       project_id        --项目id
      ,sub_org_id        --机构id
      ,max(project_name)     as project_name       --项目名称
      ,max(project_type_id)  as project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_month) as plan_num_year  --计划总数
      ,sum(item_num_month) as item_num_year  --任务总数
      ,sum(complete_num_month) as complete_num_year --完成任务数
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_plan_snapshot_by_promonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by project_id
           ,sub_org_id
           ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_plan_snapshot_by_proday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_proday partition(part_date)
select
     project_id         --项目id
    ,sub_org_id         --机构id
    ,project_name       --项目名称
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_day  --计划总数
    ,item_num_day  --任务总数
    ,complete_num_day --完成任务数
    ,coalesce(complete_num_day/item_num_day,0) as complete_rate_day --完成率
    ,date_time  --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       project_id        --项目id
      ,sub_org_id        --机构id
      ,max(project_name)     as project_name       --项目名称
      ,max(project_type_id)  as project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_day) as plan_num_day  --计划总数
      ,sum(item_num_day) as item_num_day  --任务总数
      ,sum(complete_num_day) as complete_num_day --完成任务数
      ,date_time
  from
  (
    select
         project_id        --项目id
        ,sub_org_id        --机构id
        ,max(project_name)     as project_name       --项目名称
        ,max(project_type_id)  as project_type_id    --项目类型id
        ,max(project_type_name)as project_type_name  --项目类型名称
        ,max(sub_org_name)     as sub_org_name       --机构名称
        ,max(org_id)           as org_id             --集团id
        ,max(org_name)         as org_name           --集团名称
        ,0 as plan_num_day  --计划总数
        ,count(1) as  item_num_day  --任务总数
        ,sum(case when complete_stase = 1 then 1 else 0 end) as complete_num_day --完成任务数
        ,substr(plan_create_time,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_plan_snapshot_info
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by project_id
            ,sub_org_id
            ,substr(plan_create_time,1,10)
    union all
    select
          project_id        --项目id
         ,sub_org_id        --机构id
         ,max(project_name)     as project_name       --项目名称
         ,max(project_type_id)  as project_type_id    --项目类型id
         ,max(project_type_name)as project_type_name  --项目类型名称
         ,max(sub_org_name)     as sub_org_name       --机构名称
         ,max(org_id)           as org_id             --集团id
         ,max(org_name)         as org_name           --集团名称
         ,count(exec_date) as plan_num_day  --计划总数
         ,0 as  item_num_day  --任务总数
         ,0 as complete_num_day --完成任务数
         ,substr(exec_date,1,10) as date_time
     from db_dw_hr.clife_hr_dwd_plan_snapshot_info
     where part_date=regexp_replace(date_sub(current_date(),1),'-','')
     group by project_id
             ,sub_org_id
             ,substr(exec_date,1,10)
  )a
  group by project_id
          ,sub_org_id
          ,date_time
)t


===========================
    clife_hr_dws_service_record_by_spstmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_spstmonth partition(part_date)
select
     service_project_id --服务项目id
    ,sub_org_id         --机构id
    ,service_type       --服务类型（增值or基础）
    ,service_project_name  --服务项目名称
    ,service_type_id       --服务类型id
    ,service_type_name     --服务类型id名
    ,sub_org_name          --机构名称
    ,org_id                --集团id
    ,org_name              --集团名称
    ,old_man_num_month     --养老院老人数
    ,cost_month                    --服务总费用
    ,service_num_month             --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_month/old_man_num_month,0) as recharge_avg_month                --人均服务费
    ,coalesce(service_num_month/old_man_num_month,0) as member_recharge_avg_month  --人均服务数
    ,date_month
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
   select
        service_project_id --服务项目id
       ,sub_org_id         --机构id
       ,service_type       --服务类型（增值or基础）
       ,max(service_project_name)   as service_project_name  --服务项目名称
       ,max(service_type_id)        as service_type_id       --服务类型id
       ,max(service_type_name)      as service_type_name     --服务类型id名
       ,max(sub_org_name) as sub_org_name  --机构名称
       ,max(org_id)       as org_id        --集团id
       ,max(org_name)     as org_name      --集团名称
       ,sum(old_man_num_day)            as old_man_num_month              --养老院老人数
       ,sum(cost_day)                   as cost_month                     --服务总费用
       ,sum(service_num_day)            as service_num_month              --服务数
       ,sum(already_settle_service_num) as already_settle_service_num     --已结算服务数
       ,sum(no_settle_service_num)      as no_settle_service_num          --未结算服务数
       ,sum(wait_complete_service_num)  as wait_complete_service_num      --待完成服务数
       ,sum(complete_service_num)       as complete_service_num           --已完成服务数
       ,sum(evaluation_service_num)     as evaluation_service_num         --已评价服务数
       ,sum(evaluation_wait_num)        as evaluation_wait_num            --待评价服务数
       ,sum(evaluation_good_num)        as evaluation_good_num            --好评数
       ,sum(evaluation_middle_num)      as evaluation_middle_num          --中评数
       ,sum(evaluation_bad_num)         as evaluation_bad_num             --差评数
       ,substr(date_time,1,7) as date_month
   from db_dw_hr.clife_hr_dws_service_record_by_spstday
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by service_project_id
          ,sub_org_id
          ,service_type
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_service_record_by_spst
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_spst partition(part_date)
select
     service_project_id    --服务项目id
    ,sub_org_id            --机构id
    ,service_type          --服务类型（增值or基础）
    ,service_project_name  --服务项目名称
    ,service_type_id       --服务类型id
    ,service_type_name     --服务类型id名
    ,sub_org_name          --机构名称
    ,org_id                --集团id
    ,org_name              --集团名称
    ,old_man_num           --养老院老人数
    ,cost                          --服务总费用
    ,service_cnt                   --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost/old_man_num,0) as recharge_avg                --人均服务费
    ,coalesce(service_cnt/old_man_num,0) as member_recharge_avg  --人均服务数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
  select
       service_project_id --服务项目id
      ,sub_org_id         --机构id
      ,service_type       --服务类型（增值or基础）
      ,max(service_project_name)   as service_project_name  --服务项目名称
      ,max(service_type_id)        as service_type_id       --服务类型id
      ,max(service_type_name)      as service_type_name     --服务类型id名
      ,max(sub_org_name) as sub_org_name  --机构名称
      ,max(org_id)       as org_id        --集团id
      ,max(org_name)     as org_name      --集团名称
      ,sum(cost)         as cost                          --服务总费用
      ,sum(1)            as service_cnt                   --服务数
      ,count(distinct old_man_id) as old_man_num --养老院老人数
      ,sum(case when settlement_status =  2 then 1 else 0 end ) as  already_settle_service_num     --已结算服务数
      ,sum(case when settlement_status =  3 then 1 else 0 end ) as  no_settle_service_num          --未结算服务数
      ,sum(case when service_status = 1  then 1 else 0 end )    as  wait_complete_service_num      --待完成服务数
      ,sum(case when service_status = 2  then 1 else 0 end )    as  complete_service_num           --已完成服务数
      ,sum(case when service_status = 3  then 1 else 0 end )    as  evaluation_service_num         --已评价服务数
      ,sum(case when evaluation_status = 0  then 1 else 0 end ) as  evaluation_wait_num            --待评价服务数
      ,sum(case when evaluation_status = 1  then 1 else 0 end ) as  evaluation_good_num            --好评数
      ,sum(case when evaluation_status = 2  then 1 else 0 end ) as  evaluation_middle_num          --中评数
      ,sum(case when evaluation_status = 3  then 1 else 0 end ) as  evaluation_bad_num             --差评数
  from db_dw_hr.clife_hr_dwd_service_record_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by service_project_id
          ,sub_org_id
          ,service_type
)t


===========================
    clife_hr_dws_service_record_by_nhyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_nhyear partition(part_date)
select
     nursin_homes_id  --养老院id
    ,service_type     --服务类型（增值or基础）
    ,nursing_homes_name    --养老院名称
    ,sub_org_id    --机构id
    ,sub_org_name  --机构名称
    ,org_id        --集团id
    ,org_name      --集团名称
    ,old_man_num_year    --养老院老人数
    ,cost_year                    --服务总费用
    ,service_num_year             --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_year/old_man_num_year,0) as recharge_avg_year                --人均服务费
    ,coalesce(service_num_year/old_man_num_year,0) as member_recharge_avg_year  --人均服务数
    ,date_year
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
   select
        nursin_homes_id        --养老院id
       ,service_type           --服务类型（增值or基础）
       ,max(nursing_homes_name) as nursing_homes_name --养老院名称
       ,max(sub_org_id)         as sub_org_id--机构id
       ,max(sub_org_name)       as sub_org_name--机构名称
       ,max(org_id)             as org_id--集团id
       ,max(org_name)           as org_name--集团名称
       ,sum(old_man_num_month)            as old_man_num_year              --养老院老人数
       ,sum(cost_month)                   as cost_year                    --服务总费用
       ,sum(service_num_month)            as service_num_year             --服务数
       ,sum(already_settle_service_num) as already_settle_service_num     --已结算服务数
       ,sum(no_settle_service_num)      as no_settle_service_num          --未结算服务数
       ,sum(wait_complete_service_num)  as wait_complete_service_num      --待完成服务数
       ,sum(complete_service_num)       as complete_service_num           --已完成服务数
       ,sum(evaluation_service_num)     as evaluation_service_num         --已评价服务数
       ,sum(evaluation_wait_num)        as evaluation_wait_num            --待评价服务数
       ,sum(evaluation_good_num)        as evaluation_good_num            --好评数
       ,sum(evaluation_middle_num)      as evaluation_middle_num          --中评数
       ,sum(evaluation_bad_num)         as evaluation_bad_num             --差评数
       ,substr(date_month,1,4) as date_year
   from db_dw_hr.clife_hr_dws_service_record_by_nhmonth
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by nursin_homes_id
           ,service_type
           ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_coupon_manage_by_nhyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_coupon_manage_by_nhyear partition(part_date)
select
     nurse_home_id --养老院id
    ,max(nursing_homes_name) as nursing_homes_name --养老院名称
    ,max(sub_org_id)         as sub_org_id         --机构id
    ,max(sub_org_name)       as sub_org_name       --机构名称
    ,max(org_id)             as org_id             --集团id
    ,max(org_name)           as org_name           --集团名称
    ,max(area_id)            as area_id            --区域id 
    ,sum(deduction_coupon_num_month)    as deduction_coupon_num_year        --每年抵扣优惠券发放数量
    ,sum(discount_coupon_num_month)     as discount_coupon_num_year         --每年折扣优惠券发放数量
    ,sum(cash_coupon_num_month)         as cash_coupon_num_year             --每年现金优惠券发放数量
    ,sum(deduction_coupon_use_num_month)as deduction_coupon_use_num_year    --每年抵扣优惠券使用数量
    ,sum(discount_coupon_use_num_month) as discount_coupon_use_num_year     --每年折扣优惠券使用数量
    ,sum(cash_coupon_use_num_month)     as cash_coupon_use_num_year         --每年现金优惠券使用数量
    ,sum(deduction_coupon_cost_month)   as deduction_coupon_cost_year       --每年抵扣优惠券花费
    ,sum(discount_coupon_cost_month)    as discount_coupon_cost_year        --每年折扣优惠券花费
    ,sum(cash_coupon_cost_month)        as cash_coupon_cost_year            --每年现金优惠券花费
    ,substr(date_month,1,4) as date_year  --年份 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from db_dw_hr.clife_hr_dws_coupon_manage_by_nhmonth
where part_date = regexp_replace(date_sub(current_date(),1),'-','')
group by nurse_home_id
        ,substr(date_month,1,4)


===========================
    clife_hr_dws_activity_manage_by_somonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_somonth partition(part_date)
select
     sub_org_id             --机构id
    ,activity_type
    ,sub_org_name           --机构名称
    ,org_id                 --集团id
    ,org_name               --集团名称
    ,area_id                --区域id 
    ,reserve_cnt_month        --预定活动室次数
    ,template_cnt_month        --使用模板次数
    ,activity_cnt_month       --创建活动数
    ,activity_fee_month       --活动费用
    ,host_activity_cnt_month  --已举办活动数
    ,confirm_old_man_cnt_month--到场人次
    ,sign_old_man_cnt_month   --签到人次
    ,apply_old_man_cnt_month  --报名人次
    ,apply_activity_cnt_month --已报名活动数
    ,date_month --月份 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       activity_type
      ,sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id 
      ,sum(reserve_cnt_day)         as reserve_cnt_month        --预定活动室次数
      ,sum(template_cnt_day)         as template_cnt_month        --使用模板次数
      ,sum(activity_cnt_day)        as activity_cnt_month       --创建活动数
      ,sum(activity_fee_day)        as activity_fee_month       --活动费用
      ,sum(host_activity_cnt_day)   as host_activity_cnt_month  --已举办活动数
      ,sum(confirm_old_man_cnt_day) as confirm_old_man_cnt_month--到场人次
      ,sum(sign_old_man_cnt_day)    as sign_old_man_cnt_month   --签到人次
      ,sum(apply_old_man_cnt_day)   as apply_old_man_cnt_month  --报名人次
      ,sum(apply_activity_cnt_day)  as apply_activity_cnt_month --已报名活动数
      ,substr(date_time,1,7) as date_month --月份     
  from db_dw_hr.clife_hr_dws_activity_manage_by_soday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by activity_type
          ,sub_org_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_activity_manage_by_soyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_soyear partition(part_date)
select
     sub_org_id             --机构id
    ,activity_type
    ,sub_org_name           --机构名称
    ,org_id                 --集团id
    ,org_name               --集团名称
    ,area_id                --区域id 
    ,reserve_cnt_year        --预定活动室次数
    ,template_cnt_year        --使用模板次数
    ,activity_cnt_year       --创建活动数
    ,activity_fee_year       --活动费用
    ,host_activity_cnt_year  --已举办活动数
    ,confirm_old_man_cnt_year--到场人次
    ,sign_old_man_cnt_year   --签到人次
    ,apply_old_man_cnt_year  --报名人次
    ,apply_activity_cnt_year --已报名活动数
    ,date_year --年份 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       activity_type
      ,sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id 
      ,sum(reserve_cnt_month)         as reserve_cnt_year        --预定活动室次数
      ,sum(template_cnt_month)         as template_cnt_year        --使用模板次数
      ,sum(activity_cnt_month)        as activity_cnt_year       --创建活动数
      ,sum(activity_fee_month)        as activity_fee_year       --活动费用
      ,sum(host_activity_cnt_month)   as host_activity_cnt_year  --已举办活动数
      ,sum(confirm_old_man_cnt_month) as confirm_old_man_cnt_year--到场人次
      ,sum(sign_old_man_cnt_month)    as sign_old_man_cnt_year   --签到人次
      ,sum(apply_old_man_cnt_month)   as apply_old_man_cnt_year  --报名人次
      ,sum(apply_activity_cnt_month)  as apply_activity_cnt_year --已报名活动数
      ,substr(date_month,1,4) as date_year --年份     
  from db_dw_hr.clife_hr_dws_activity_manage_by_somonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by activity_type
          ,sub_org_id 
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_activity_manage_by_nhyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_nhyear partition(part_date)
select
     nurse_home_id --养老院id
    ,nursing_homes_name     --养老院名称
    ,sub_org_id             --机构id
    ,sub_org_name           --机构名称
    ,org_id                 --集团id
    ,org_name               --集团名称
    ,area_id                --区域id 
    ,reserve_cnt_year        --预定活动室次数
    ,template_cnt_year        --使用模板次数
    ,activity_cnt_year       --创建活动数
    ,activity_fee_year       --活动费用
    ,host_activity_cnt_year  --已举办活动数
    ,confirm_old_man_cnt_year--到场人次
    ,sign_old_man_cnt_year   --签到人次
    ,apply_old_man_cnt_year  --报名人次
    ,apply_activity_cnt_year --已报名活动数
    ,date_year --年份 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       nurse_home_id --养老院id
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id         --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id 
      ,sum(reserve_cnt_month)         as reserve_cnt_year        --预定活动室次数
      ,sum(template_cnt_month)         as template_cnt_year        --使用模板次数
      ,sum(activity_cnt_month)        as activity_cnt_year       --创建活动数
      ,sum(activity_fee_month)        as activity_fee_year       --活动费用
      ,sum(host_activity_cnt_month)   as host_activity_cnt_year  --已举办活动数
      ,sum(confirm_old_man_cnt_month) as confirm_old_man_cnt_year--到场人次
      ,sum(sign_old_man_cnt_month)    as sign_old_man_cnt_year   --签到人次
      ,sum(apply_old_man_cnt_month)   as apply_old_man_cnt_year  --报名人次
      ,sum(apply_activity_cnt_month)  as apply_activity_cnt_year --已报名活动数
      ,substr(date_month,1,4) as date_year --年份     
  from db_dw_hr.clife_hr_dws_activity_manage_by_nhmonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nurse_home_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_reside_manage_by_somonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_somonth partition(part_date)
select
     sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_month        --每月入住人数
    ,countermand_cnt_month   --每月退住人数
    ,room_num_month          --每月入住房间数
    ,bed_num_month           --每月入住床位数
    ,all_num_month           --每月总人数
    ,visit_num_month         --每月探访人数
    ,visit_time_month        --每月探访时长
    ,leave_num_month         --每月请假次数
    ,leave_time_month      --每月请假时长
    ,sick_leave_num_month  --每月病假次数
    ,casual_leave_num_month--每月事假次数
    ,other_leave_num_month --每月其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_month/all_num_month,0)           as check_in_rate_month      --每月入住率
    ,coalesce(countermand_cnt_month/all_num_month,0)      as countermand_rate_month   --每月退住率
    ,coalesce(room_num_month/room_amount,0)               as check_in_room_rate_month --每月房间入住率
    ,coalesce((room_amount-room_num_month)/room_amount,0) as vacant_room_rate_month   --每月房间空房率
    ,coalesce(bed_num_month/bed_amount,0)                 as check_in_bed_rate_month  --每月床位入住率
    ,coalesce((bed_amount-bed_num_month)/bed_amount,0)    as vacant_bed_rate_month    --每月床位空床率	
    ,contract_time_1_num_month        --录入合同数量（1年期）
    ,contract_time_2_num_month        --录入合同数量（2年期）
    ,contract_time_3_num_month        --录入合同数量（3年期）
    ,contract_time_4_num_month        --录入合同数量（4年期）
    ,contract_time_5_num_month        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_month --录入合同数量（5年以上）
    ,refund_amount        --当月请假退费金额	
    ,leave_reducation_fee --当月请假减免费用	
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select
       sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)       as reside_cnt_month        --每月入住人数
      ,sum(countermand_cnt_day)  as countermand_cnt_month   --每月退住人数
      ,sum(room_num_day)         as room_num_month          --每月入住房间数
      ,sum(bed_num_day)          as bed_num_month           --每月入住床位数
      ,sum(all_num)              as all_num_month           --每月总人数
      ,sum(visit_num_day)        as visit_num_month         --每月探访人数
      ,sum(visit_time_day)       as visit_time_month        --每月探访时长
      ,sum(leave_num_day)        as leave_num_month         --每月请假次数
      ,sum(leave_time_day)       as leave_time_month      --每月请假时长
      ,sum(sick_leave_num_day)   as sick_leave_num_month  --每月病假次数
      ,sum(casual_leave_num_day) as casual_leave_num_month--每月事假次数
      ,sum(other_leave_num_day)  as other_leave_num_month --每月其他假次数
      ,sum(refund_amount)        as refund_amount        --当月请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当月请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_day)         as contract_time_1_num_month       --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)         as contract_time_2_num_month       --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)         as contract_time_3_num_month       --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)         as contract_time_4_num_month       --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)         as contract_time_5_num_month       --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day) as contract_time_surpass_5_num_month--录入合同数量（5年以上）
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_reside_manage_by_soday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by sub_org_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_reside_manage_by_nhyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_nhyear partition(part_date)
select
     nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_year         --每年入住人数
    ,countermand_cnt_year    --每年退住人数
    ,room_num_year           --每年入住房间数
    ,bed_num_year            --每年入住床位数
    ,all_num_year            --每年总人数
    ,visit_num_year          --每年探访人数
    ,visit_time_year         --每年探访时长
    ,leave_num_year          --每年请假次数
    ,leave_time_year         --每年请假时长
    ,sick_leave_num_year     --每年病假次数
    ,casual_leave_num_year   --每年事假次数
    ,other_leave_num_year    --每年其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_year/all_num_year,0)           as check_in_rate_year      --每年入住率
    ,coalesce(countermand_cnt_year/all_num_year,0)      as countermand_rate_year   --每年退住率
    ,coalesce(room_num_year/room_amount,0)               as check_in_room_rate_year--每年房间入住率
    ,coalesce((room_amount-room_num_year)/room_amount,0) as vacant_room_rate_year  --每年房间空房率
    ,coalesce(bed_num_year/bed_amount,0)                 as check_in_bed_rate_year --每年床位入住率
    ,coalesce((bed_amount-bed_num_year)/bed_amount,0)    as vacant_bed_rate_year   --每年床位空床率	
    ,contract_time_1_num_year        --录入合同数量（1年期）
    ,contract_time_2_num_year        --录入合同数量（2年期）
    ,contract_time_3_num_year        --录入合同数量（3年期）
    ,contract_time_4_num_year        --录入合同数量（4年期）
    ,contract_time_5_num_year        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_year --录入合同数量（5年以上）
    ,refund_amount        --当年请假退费金额	
    ,leave_reducation_fee --当年请假减免费用
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_month)       as reside_cnt_year         --每年入住人数
      ,sum(countermand_cnt_month)  as countermand_cnt_year    --每年退住人数
      ,sum(room_num_month)         as room_num_year           --每年入住房间数
      ,sum(bed_num_month)          as bed_num_year            --每年入住床位数
      ,sum(all_num_month)          as all_num_year            --每年总人数
      ,sum(visit_num_month)        as visit_num_year          --每年探访人数
      ,sum(visit_time_month)       as visit_time_year         --每年探访时长
      ,sum(leave_num_month)        as leave_num_year          --每年请假次数
      ,sum(leave_time_month)       as leave_time_year         --每年请假时长
      ,sum(sick_leave_num_month)   as sick_leave_num_year     --每年病假次数
      ,sum(casual_leave_num_month) as casual_leave_num_year   --每年事假次数
      ,sum(other_leave_num_month)  as other_leave_num_year    --每年其他假次数
      ,sum(refund_amount)        as refund_amount        --当年请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当年请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_month)         contract_time_1_num_year        --录入合同数量（1年期）
      ,sum(contract_time_2_num_month)         contract_time_2_num_year        --录入合同数量（2年期）
      ,sum(contract_time_3_num_month)         contract_time_3_num_year        --录入合同数量（3年期）
      ,sum(contract_time_4_num_month)         contract_time_4_num_year        --录入合同数量（4年期）
      ,sum(contract_time_5_num_month)         contract_time_5_num_year        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_month) contract_time_surpass_5_num_year--录入合同数量（5年以上）
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_reside_manage_by_nhmonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_reside_manage_by_nhmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_nhmonth partition(part_date)
select
     nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_month        --每月入住人数
    ,countermand_cnt_month   --每月退住人数
    ,room_num_month          --每月入住房间数
    ,bed_num_month           --每月入住床位数
    ,all_num_month           --每月总人数
    ,visit_num_month         --每月探访人数
    ,visit_time_month        --每月探访时长
    ,leave_num_month         --每月请假次数
    ,leave_time_month      --每月请假时长
    ,sick_leave_num_month  --每月病假次数
    ,casual_leave_num_month--每月事假次数
    ,other_leave_num_month --每月其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_month/all_num_month,0)           as check_in_rate_month      --每月入住率
    ,coalesce(countermand_cnt_month/all_num_month,0)      as countermand_rate_month   --每月退住率
    ,coalesce(room_num_month/room_amount,0)               as check_in_room_rate_month --每月房间入住率
    ,coalesce((room_amount-room_num_month)/room_amount,0) as vacant_room_rate_month   --每月房间空房率
    ,coalesce(bed_num_month/bed_amount,0)                 as check_in_bed_rate_month  --每月床位入住率
    ,coalesce((bed_amount-bed_num_month)/bed_amount,0)    as vacant_bed_rate_month    --每月床位空床率	
    ,contract_time_1_num_month        --录入合同数量（1年期）
    ,contract_time_2_num_month        --录入合同数量（2年期）
    ,contract_time_3_num_month        --录入合同数量（3年期）
    ,contract_time_4_num_month        --录入合同数量（4年期）
    ,contract_time_5_num_month        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_month --录入合同数量（5年以上）
    ,refund_amount        --当月请假退费金额	
    ,leave_reducation_fee --当月请假减免费用	
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)       as reside_cnt_month        --每月入住人数
      ,sum(countermand_cnt_day)  as countermand_cnt_month   --每月退住人数
      ,sum(room_num_day)         as room_num_month          --每月入住房间数
      ,sum(bed_num_day)          as bed_num_month           --每月入住床位数
      ,sum(all_num)              as all_num_month           --每月总人数
      ,sum(visit_num_day)        as visit_num_month         --每月探访人数
      ,sum(visit_time_day)       as visit_time_month        --每月探访时长
      ,sum(leave_num_day)        as leave_num_month         --每月请假次数
      ,sum(leave_time_day)       as leave_time_month      --每月请假时长
      ,sum(sick_leave_num_day)   as sick_leave_num_month  --每月病假次数
      ,sum(casual_leave_num_day) as casual_leave_num_month--每月事假次数
      ,sum(other_leave_num_day)  as other_leave_num_month --每月其他假次数
      ,sum(refund_amount)        as refund_amount        --当月请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当月请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_day)         as contract_time_1_num_month       --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)         as contract_time_2_num_month       --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)         as contract_time_3_num_month       --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)         as contract_time_4_num_month       --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)         as contract_time_5_num_month       --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day) as contract_time_surpass_5_num_month--录入合同数量（5年以上）
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_reside_manage_by_nhday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_reside_manage_by_floor
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_floor partition(part_date)
select
     t.floor_id            --楼层id
    ,t.building_id         --楼栋id
    ,t.nursin_homes_id     --养老院id
    ,t.nursing_homes_name  --养老院名称
    ,t.sub_org_id          --机构id
    ,t.sub_org_name        --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.check_in_num        --入住人数
    ,t.countermand_num     --退住人数
    ,coalesce(t.check_in_num/t.all_num,0) as  check_in_rate       --入住率
    ,coalesce(t.countermand_num/t.all_num,0) as countermand_rate  --退住率
    ,t2.room_num       --入住房间数
    ,t3.room_amount    --楼层房间数
    ,coalesce(t2.room_num/t3.room_amount,0) as check_in_room_rate               --房间入住率
    ,coalesce((t3.room_amount-t2.room_num)/t3.room_amount,0) as vacant_room_rate --房间空房率
    ,t2.bed_num       --入住床位数
    ,t3.bed_amount    --楼层床位数
    ,coalesce(t2.bed_num/t3.bed_amount,0) as check_in_bed_rate              --床位入住率
    ,coalesce((t3.bed_amount-t2.bed_num)/t3.bed_amount,0) as vacant_bed_rate --床位空床率
    ,t.relatives_num --探访人数
    ,t1.visit_time   --探访时长
    ,t.contract_time_1_num        --录入合同数量（1年期）
    ,t.contract_time_2_num        --录入合同数量（2年期）
    ,t.contract_time_3_num        --录入合同数量（3年期）
    ,t.contract_time_4_num        --录入合同数量（4年期）
    ,t.contract_time_5_num        --录入合同数量（5年期）
    ,t.contract_time_surpass_5_num --录入合同数量（5年以上）
    ,t.leave_num        --请假次数
    ,t.leave_time       --请假时长
    ,t.sick_leave_num   --病假次数
    ,t.casual_leave_num --事假次数
    ,t.other_leave_num  --其他假次数
    ,t.all_num          --总人数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       floor_id                                       --楼层id
      ,max(building_id)        as building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as  area_id            --区域id
      ,count(1)                as all_num             --所有人数
      ,sum(check_in_num) as check_in_num    --入住人数
      ,sum(countermand_num) as countermand_num --退住人数
      ,sum(relatives_num) as relatives_num --探访人数
      ,sum(contract_time_1_num) as contract_time_1_num        --录入合同数量（1年期）
      ,sum(contract_time_2_num) as contract_time_2_num        --录入合同数量（2年期）
      ,sum(contract_time_3_num) as contract_time_3_num        --录入合同数量（3年期）
      ,sum(contract_time_4_num) as contract_time_4_num        --录入合同数量（4年期）
      ,sum(contract_time_5_num) as contract_time_5_num        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num) as contract_time_surpass_5_num --录入合同数量（5年以上）
      ,sum(leave_num)        as leave_num --请假次数
      ,sum(leave_time)       as leave_time --请假时长
      ,sum(sick_leave_num)   as sick_leave_num     --病假次数
      ,sum(casual_leave_num) as casual_leave_num --事假次数
      ,sum(other_leave_num)  as other_leave_num   --其他假次数
  from db_dw_hr.clife_hr_dwm_reside_manage_aggregation
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by floor_id
)t 
left join
(
  select
       floor_id
      ,sum(CAST((if(a.visit_end_time is null,unix_timestamp(),unix_timestamp(a.visit_end_time)) - unix_timestamp(a.visit_start_time)) / 60 AS int) % 60) as visit_time --探访时长
  from
  (
     select
          b.old_man_id,b.floor_id,b.building_id
         ,ip.relatives_name
         ,split(ip.visit_time,'-')[0] as visit_start_time
         ,split(ip.visit_time,'-')[1] as visit_end_time
    from db_dw_hr.clife_hr_dwm_reside_manage_aggregation b
    lateral view explodemulti(if(b.relatives_names is null,'',b.relatives_names),if(b.visit_times is null,'',b.visit_times))ip as relatives_name,visit_time
    where b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  ) a
  group by floor_id
)t1 on t.floor_id = t1.floor_id
left join
(
  select
       floor_id
      ,count(case when a.reside_status = 3 then a.room_id else null end)   as room_num    --入住房间数
      ,count(case when a.reside_status = 3 then a.bed_id else null end)   as bed_num     --入住床位数
  from
  (
    select
         b.old_man_id,b.floor_id,b.building_id
        ,ip.reside_status
        ,ip.room_id
        ,ip.bed_id
    from db_dw_hr.clife_hr_dwm_reside_manage_aggregation b
    lateral view explodemulti(if(b.reside_status_list is null,'',b.reside_status_list),if(b.room_id_list is null,'',b.room_id_list),if(b.bed_id_list is null,'',b.bed_id_list))ip as reside_status,room_id,bed_id
    where b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  ) a
  group by floor_id
)t2 on t.floor_id = t2.floor_id
left join
(
  select
       floor_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by floor_id
)t3 on t.floor_id=t3.floor_id


===========================
    clife_hr_dws_reside_manage_by_bday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_bday partition(part_date)
select
     a.building_id         --楼栋id
    ,a.nursin_homes_id     --养老院id
    ,a.nursing_homes_name  --养老院名称
    ,a.sub_org_id          --机构id
    ,a.sub_org_name        --机构名称
    ,a.org_id              --集团id
    ,a.org_name            --集团名称
    ,a.area_id             --区域id
    ,a.reside_cnt_day       --每天入住人数
    ,a.countermand_cnt_day  --每日退住人数
    ,a.room_num_day         --每日入住房间数
    ,a.bed_num_day          --每日入住床位数
    ,a.all_num              --每日总人数
    ,a.visit_num_day        --每日探访人数
    ,a.visit_time_day       --每日探访时长
    ,a.leave_num_day        --每日请假次数
    ,a.leave_time_day       --每日请假时长
    ,a.sick_leave_num_day   --每日病假次数
    ,a.casual_leave_num_day --每日事假次数
    ,a.other_leave_num_day  --每日其他假次数
    ,b.room_amount --楼层房间数
    ,b.bed_amount  --楼层床位数
    ,coalesce(a.reside_cnt_day/a.all_num,0)      as check_in_rate_day       --每日入住率
    ,coalesce(a.countermand_cnt_day/a.all_num,0) as countermand_rate_day    --每日退住率
    ,coalesce(a.room_num_day/b.room_amount,0)    as check_in_room_rate_day  --每日房间入住率
    ,coalesce((b.room_amount-a.room_num_day)/b.room_amount,0) as vacant_room_rate_day  --房间空房率
    ,coalesce(a.bed_num_day/b.bed_amount,0)                   as check_in_bed_rate_day --床位入住率
    ,coalesce((b.bed_amount-a.bed_num_day)/b.bed_amount,0)    as vacant_bed_rate_day   --床位空床率	
    ,a.contract_time_1_num_day        --录入合同数量（1年期）
    ,a.contract_time_2_num_day        --录入合同数量（2年期）
    ,a.contract_time_3_num_day        --录入合同数量（3年期）
    ,a.contract_time_4_num_day        --录入合同数量（4年期）
    ,a.contract_time_5_num_day        --录入合同数量（5年期）
    ,a.contract_time_surpass_5_num_day --录入合同数量（5年以上）
    ,a.refund_amount        --当天请假退费金额
    ,a.leave_reducation_fee --当天请假减免费用
    ,a.date_time --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       building_id          --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)     as reside_cnt_day
      ,sum(countermand_cnt_day) as countermand_cnt_day
      ,sum(room_num_day)        as room_num_day
      ,sum(bed_num_day)         as bed_num_day
      ,sum(all_num)               as all_num
      ,sum(visit_num_day)         as visit_num_day       
      ,sum(visit_time_day)        as visit_time_day      
      ,sum(leave_num_day)         as leave_num_day       
      ,sum(leave_time_day)        as leave_time_day      
      ,sum(sick_leave_num_day)    as sick_leave_num_day  
      ,sum(casual_leave_num_day)  as casual_leave_num_day
      ,sum(other_leave_num_day)   as other_leave_num_day 
      ,sum(refund_amount)         as refund_amount
      ,sum(leave_reducation_fee)  as leave_reducation_fee
      ,sum(contract_time_1_num_day)        as contract_time_1_num_day        --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)        as contract_time_2_num_day        --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)        as contract_time_3_num_day        --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)        as contract_time_4_num_day        --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)        as contract_time_5_num_day        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day)as contract_time_surpass_5_num_day--录入合同数量（5年以上）
      ,date_time --日期
  from db_dw_hr.clife_hr_dws_reside_manage_by_fday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
          ,date_time
)a
left join
(
  select
       building_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
)b on a.building_id=b.building_id


===========================
    clife_hr_dws_reside_manage_by_nhday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_nhday partition(part_date)
select
     a.nursin_homes_id     --养老院id
    ,a.nursing_homes_name  --养老院名称
    ,a.sub_org_id          --机构id
    ,a.sub_org_name        --机构名称
    ,a.org_id              --集团id
    ,a.org_name            --集团名称
    ,a.area_id             --区域id
    ,a.reside_cnt_day       --每天入住人数
    ,a.countermand_cnt_day  --每日退住人数
    ,a.room_num_day         --每日入住房间数
    ,a.bed_num_day          --每日入住床位数
    ,a.all_num              --每日总人数
    ,a.visit_num_day        --每日探访人数
    ,a.visit_time_day       --每日探访时长
    ,a.leave_num_day        --每日请假次数
    ,a.leave_time_day       --每日请假时长
    ,a.sick_leave_num_day   --每日病假次数
    ,a.casual_leave_num_day --每日事假次数
    ,a.other_leave_num_day  --每日其他假次数
    ,b.room_amount --楼层房间数
    ,b.bed_amount  --楼层床位数
    ,coalesce(a.reside_cnt_day/a.all_num,0)      as check_in_rate_day       --每日入住率
    ,coalesce(a.countermand_cnt_day/a.all_num,0) as countermand_rate_day    --每日退住率
    ,coalesce(a.room_num_day/b.room_amount,0)    as check_in_room_rate_day  --每日房间入住率
    ,coalesce((b.room_amount-a.room_num_day)/b.room_amount,0) as vacant_room_rate_day  --房间空房率
    ,coalesce(a.bed_num_day/b.bed_amount,0)                   as check_in_bed_rate_day --床位入住率
    ,coalesce((b.bed_amount-a.bed_num_day)/b.bed_amount,0)    as vacant_bed_rate_day   --床位空床率	
    ,a.contract_time_1_num_day        --录入合同数量（1年期）
    ,a.contract_time_2_num_day        --录入合同数量（2年期）
    ,a.contract_time_3_num_day        --录入合同数量（3年期）
    ,a.contract_time_4_num_day        --录入合同数量（4年期）
    ,a.contract_time_5_num_day        --录入合同数量（5年期）
    ,a.contract_time_surpass_5_num_day --录入合同数量（5年以上）
    ,a.refund_amount        --当天请假退费金额
    ,a.leave_reducation_fee --当天请假减免费用
    ,a.date_time --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)     as reside_cnt_day
      ,sum(countermand_cnt_day) as countermand_cnt_day
      ,sum(room_num_day)        as room_num_day
      ,sum(bed_num_day)         as bed_num_day
      ,sum(all_num)               as all_num
      ,sum(visit_num_day)         as visit_num_day       
      ,sum(visit_time_day)        as visit_time_day      
      ,sum(leave_num_day)         as leave_num_day       
      ,sum(leave_time_day)        as leave_time_day      
      ,sum(sick_leave_num_day)    as sick_leave_num_day  
      ,sum(casual_leave_num_day)  as casual_leave_num_day
      ,sum(other_leave_num_day)   as other_leave_num_day 
      ,sum(refund_amount)         as refund_amount
      ,sum(leave_reducation_fee)  as leave_reducation_fee
      ,sum(contract_time_1_num_day)        as contract_time_1_num_day        --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)        as contract_time_2_num_day        --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)        as contract_time_3_num_day        --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)        as contract_time_4_num_day        --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)        as contract_time_5_num_day        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day)as contract_time_surpass_5_num_day--录入合同数量（5年以上）
      ,date_time
  from db_dw_hr.clife_hr_dws_reside_manage_by_bday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
          ,date_time
)a
left join
(
  select
       nursing_homes_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursing_homes_id
)b on a.nursin_homes_id=b.nursing_homes_id


===========================
    clife_hr_dws_recharge_consumption_by_asday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_asday partition(part_date)
select
     sub_org_id              --机构id
    ,account_name            --账户名称
    ,sub_org_name            --机构名称
    ,org_id                  --集团id
    ,org_name                --集团名称
    ,account_old_man_cnt     --账户老人数
    ,account_member_cnt      --账户会员数
    ,check_in_payment_num_day        --每天入住缴费次数
    ,check_in_payment_amount_day     --每天入住缴费金额
    ,recharge_num_day                --每天账户充值次数
    ,recharge_amount_day             --每天账户充值金额
    ,member_recharge_num_day         --每天会员充值次数
    ,member_recharge_amount_day      --每天会员充值金额
    ,consume_num_day                 --每天账户消费次数
    ,consume_amount_day              --每天账户消费金额
    ,member_consume_num_day          --每天会员消费次数
    ,member_consume_amount_day       --每天会员消费金额
    ,countermand_amount_day          --每天退住退费金额
    ,coalesce(recharge_amount_day/recharge_old_man_cnt,0) as recharge_avg_day --人均充值金额
    ,coalesce(member_recharge_amount_day/recharge_member_cnt,0) as member_recharge_avg_day  --会员人均充值金额
    ,date_time --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select 
       b.account_name
      ,a.sub_org_id
      ,max(a.sub_org_name)       as sub_org_name            --机构名称
      ,max(a.org_id)             as org_id                  --集团id
      ,max(a.org_name)           as org_name                --集团名称
      ,count(distinct a.old_man_id) as account_old_man_cnt  --账户老人数
      ,count(distinct a.member_id)  as account_member_cnt   --账户会员数
      ,sum(case when a.behavior_type='入住缴费' then 1 else 0 end)      as check_in_payment_num_day      --每天入住缴费次数
      ,sum(case when a.behavior_type='入住缴费' then a.amount else 0 end) as check_in_payment_amount_day   --每天入住缴费金额
      ,sum(case when a.behavior_type='账户充值' then 1 else 0 end)      as recharge_num_day              --每天账户充值次数
      ,sum(case when a.behavior_type='账户充值' then a.amount else 0 end) as recharge_amount_day           --每天账户充值金额
      ,count(case when a.behavior_type='账户充值' then a.old_man_id else null end) as recharge_old_man_cnt --每天基础充值老人数
      ,sum(case when a.behavior_type='会员充值' then 1 else 0 end)      as member_recharge_num_day       --每天会员充值次数
      ,sum(case when a.behavior_type='会员充值' then a.amount else 0 end) as member_recharge_amount_day    --每天会员充值金额
      ,count(case when a.behavior_type='会员充值' then a.old_man_id else null end) as recharge_member_cnt  --每天会员充值老人数
      ,sum(case when a.behavior_type='账户消费' then 1 else 0 end)      as consume_num_day               --每天账户消费次数
      ,sum(case when a.behavior_type='账户消费' then a.amount else 0 end) as consume_amount_day            --每天账户消费金额
      ,sum(case when a.behavior_type='会员消费' then 1 else 0 end)      as member_consume_num_day        --每天账户消费次数
      ,sum(case when a.behavior_type='会员消费' then a.amount else 0 end) as member_consume_amount_day     --每天会员消费金额
      ,sum(case when a.behavior_type='退住退费' then a.amount else 0 end) as countermand_amount_day        --每天退住退费金额
      ,substr(a.recharge_consumption_time,1,10) as date_time
  from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
  left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  group by b.account_name
          ,a.sub_org_id
          ,substr(a.recharge_consumption_time,1,10)
)t


===========================
    clife_hr_dws_reside_manage_by_bmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_bmonth partition(part_date)
select
     building_id         --楼栋id
    ,nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_month        --每月入住人数
    ,countermand_cnt_month   --每月退住人数
    ,room_num_month          --每月入住房间数
    ,bed_num_month           --每月入住床位数
    ,all_num_month           --每月总人数
    ,visit_num_month         --每月探访人数
    ,visit_time_month        --每月探访时长
    ,leave_num_month         --每月请假次数
    ,leave_time_month      --每月请假时长
    ,sick_leave_num_month  --每月病假次数
    ,casual_leave_num_month--每月事假次数
    ,other_leave_num_month --每月其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_month/all_num_month,0)           as check_in_rate_month      --每月入住率
    ,coalesce(countermand_cnt_month/all_num_month,0)      as countermand_rate_month   --每月退住率
    ,coalesce(room_num_month/room_amount,0)               as check_in_room_rate_month --每月房间入住率
    ,coalesce((room_amount-room_num_month)/room_amount,0) as vacant_room_rate_month   --每月房间空房率
    ,coalesce(bed_num_month/bed_amount,0)                 as check_in_bed_rate_month  --每月床位入住率
    ,coalesce((bed_amount-bed_num_month)/bed_amount,0)    as vacant_bed_rate_month    --每月床位空床率	
    ,contract_time_1_num_month        --录入合同数量（1年期）
    ,contract_time_2_num_month        --录入合同数量（2年期）
    ,contract_time_3_num_month        --录入合同数量（3年期）
    ,contract_time_4_num_month        --录入合同数量（4年期）
    ,contract_time_5_num_month        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_month --录入合同数量（5年以上）
    ,refund_amount        --当月请假退费金额	
    ,leave_reducation_fee --当月请假减免费用	
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)       as reside_cnt_month        --每月入住人数
      ,sum(countermand_cnt_day)  as countermand_cnt_month   --每月退住人数
      ,sum(room_num_day)         as room_num_month          --每月入住房间数
      ,sum(bed_num_day)          as bed_num_month           --每月入住床位数
      ,sum(all_num)              as all_num_month           --每月总人数
      ,sum(visit_num_day)        as visit_num_month         --每月探访人数
      ,sum(visit_time_day)       as visit_time_month        --每月探访时长
      ,sum(leave_num_day)        as leave_num_month         --每月请假次数
      ,sum(leave_time_day)       as leave_time_month      --每月请假时长
      ,sum(sick_leave_num_day)   as sick_leave_num_month  --每月病假次数
      ,sum(casual_leave_num_day) as casual_leave_num_month--每月事假次数
      ,sum(other_leave_num_day)  as other_leave_num_month --每月其他假次数
      ,sum(refund_amount)        as refund_amount        --当月请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当月请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_day)         as contract_time_1_num_month        --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)         as contract_time_2_num_month        --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)         as contract_time_3_num_month        --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)         as contract_time_4_num_month        --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)         as contract_time_5_num_month        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day ) as contract_time_surpass_5_num_month--录入合同数量（5年以上）
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_reside_manage_by_bday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_reside_manage_by_fyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_fyear partition(part_date)
select
     floor_id            --楼层id
    ,building_id         --楼栋id
    ,nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_year         --每年入住人数
    ,countermand_cnt_year    --每年退住人数
    ,room_num_year           --每年入住房间数
    ,bed_num_year            --每年入住床位数
    ,all_num_year            --每年总人数
    ,visit_num_year          --每年探访人数
    ,visit_time_year         --每年探访时长
    ,leave_num_year          --每年请假次数
    ,leave_time_year         --每年请假时长
    ,sick_leave_num_year     --每年病假次数
    ,casual_leave_num_year   --每年事假次数
    ,other_leave_num_year    --每年其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_year/all_num_year,0)           as check_in_rate_year      --每年入住率
    ,coalesce(countermand_cnt_year/all_num_year,0)      as countermand_rate_year   --每年退住率
    ,coalesce(room_num_year/room_amount,0)               as check_in_room_rate_year--每年房间入住率
    ,coalesce((room_amount-room_num_year)/room_amount,0) as vacant_room_rate_year  --每年房间空房率
    ,coalesce(bed_num_year/bed_amount,0)                 as check_in_bed_rate_year --每年床位入住率
    ,coalesce((bed_amount-bed_num_year)/bed_amount,0)    as vacant_bed_rate_year   --每年床位空床率	
    ,contract_time_1_num_year        --录入合同数量（1年期）
    ,contract_time_2_num_year        --录入合同数量（2年期）
    ,contract_time_3_num_year        --录入合同数量（3年期）
    ,contract_time_4_num_year        --录入合同数量（4年期）
    ,contract_time_5_num_year        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_year --录入合同数量（5年以上）
    ,refund_amount        --当年请假退费金额	
    ,leave_reducation_fee --当年请假减免费用
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       floor_id            --楼层id
      ,max(building_id)        as building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_month)       as reside_cnt_year         --每年入住人数
      ,sum(countermand_cnt_month)  as countermand_cnt_year    --每年退住人数
      ,sum(room_num_month)         as room_num_year           --每年入住房间数
      ,sum(bed_num_month)          as bed_num_year            --每年入住床位数
      ,sum(all_num_month)          as all_num_year            --每年总人数
      ,sum(visit_num_month)        as visit_num_year          --每年探访人数
      ,sum(visit_time_month)       as visit_time_year         --每年探访时长
      ,sum(leave_num_month)        as leave_num_year          --每年请假次数
      ,sum(leave_time_month)       as leave_time_year         --每年请假时长
      ,sum(sick_leave_num_month)   as sick_leave_num_year     --每年病假次数
      ,sum(casual_leave_num_month) as casual_leave_num_year   --每年事假次数
      ,sum(other_leave_num_month)  as other_leave_num_year    --每年其他假次数
      ,sum(refund_amount)        as refund_amount        --当年请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当年请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_month)         contract_time_1_num_year        --录入合同数量（1年期）
      ,sum(contract_time_2_num_month)         contract_time_2_num_year        --录入合同数量（2年期）
      ,sum(contract_time_3_num_month)         contract_time_3_num_year        --录入合同数量（3年期）
      ,sum(contract_time_4_num_month)         contract_time_4_num_year        --录入合同数量（4年期）
      ,sum(contract_time_5_num_month)         contract_time_5_num_year        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_month) contract_time_surpass_5_num_year--录入合同数量（5年以上）
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_reside_manage_by_fmonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by floor_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_reside_manage_by_fmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_fmonth partition(part_date)
select
     floor_id            --楼层id
    ,building_id         --楼栋id
    ,nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_month        --每月入住人数
    ,countermand_cnt_month   --每月退住人数
    ,room_num_month          --每月入住房间数
    ,bed_num_month           --每月入住床位数
    ,all_num_month           --每月总人数
    ,visit_num_month         --每月探访人数
    ,visit_time_month        --每月探访时长
    ,leave_num_month         --每月请假次数
    ,leave_time_month      --每月请假时长
    ,sick_leave_num_month  --每月病假次数
    ,casual_leave_num_month--每月事假次数
    ,other_leave_num_month --每月其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_month/all_num_month,0)           as check_in_rate_month      --每月入住率
    ,coalesce(countermand_cnt_month/all_num_month,0)      as countermand_rate_month   --每月退住率
    ,coalesce(room_num_month/room_amount,0)               as check_in_room_rate_month --每月房间入住率
    ,coalesce((room_amount-room_num_month)/room_amount,0) as vacant_room_rate_month   --每月房间空房率
    ,coalesce(bed_num_month/bed_amount,0)                 as check_in_bed_rate_month  --每月床位入住率
    ,coalesce((bed_amount-bed_num_month)/bed_amount,0)    as vacant_bed_rate_month    --每月床位空床率	
    ,contract_time_1_num_month        --录入合同数量（1年期）
    ,contract_time_2_num_month        --录入合同数量（2年期）
    ,contract_time_3_num_month        --录入合同数量（3年期）
    ,contract_time_4_num_month        --录入合同数量（4年期）
    ,contract_time_5_num_month        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_month --录入合同数量（5年以上）
    ,refund_amount        --当月请假退费金额	
    ,leave_reducation_fee --当月请假减免费用	
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       floor_id            --楼层id
      ,max(building_id)        as building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_day)       as reside_cnt_month        --每月入住人数
      ,sum(countermand_cnt_day)  as countermand_cnt_month   --每月退住人数
      ,sum(room_num_day)         as room_num_month          --每月入住房间数
      ,sum(bed_num_day)          as bed_num_month           --每月入住床位数
      ,sum(all_num)              as all_num_month           --每月总人数
      ,sum(visit_num_day)        as visit_num_month         --每月探访人数
      ,sum(visit_time_day)       as visit_time_month        --每月探访时长
      ,sum(leave_num_day)        as leave_num_month         --每月请假次数
      ,sum(leave_time_day)       as leave_time_month      --每月请假时长
      ,sum(sick_leave_num_day)   as sick_leave_num_month  --每月病假次数
      ,sum(casual_leave_num_day) as casual_leave_num_month--每月事假次数
      ,sum(other_leave_num_day)  as other_leave_num_month --每月其他假次数
      ,sum(refund_amount)        as refund_amount        --当月请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当月请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_day)         as contract_time_1_num_month       --录入合同数量（1年期）
      ,sum(contract_time_2_num_day)         as contract_time_2_num_month       --录入合同数量（2年期）
      ,sum(contract_time_3_num_day)         as contract_time_3_num_month       --录入合同数量（3年期）
      ,sum(contract_time_4_num_day)         as contract_time_4_num_month       --录入合同数量（4年期）
      ,sum(contract_time_5_num_day)         as contract_time_5_num_month       --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_day) as contract_time_surpass_5_num_month--录入合同数量（5年以上）
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_reside_manage_by_fday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by floor_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_reside_manage_by_building
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_building partition(part_date)
select
     t.building_id         --楼栋id
    ,t.nursin_homes_id     --养老院id
    ,t.nursing_homes_name  --养老院名称
    ,t.sub_org_id          --机构id
    ,t.sub_org_name        --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.check_in_num        --入住人数
    ,t.countermand_num     --退住人数
    ,coalesce(t.check_in_num/t.all_num,0) as  check_in_rate       --入住率
    ,coalesce(t.countermand_num/t.all_num,0) as countermand_rate  --退住率
    ,t.room_num       --入住房间数
    ,t3.room_amount    --楼层房间数
    ,coalesce(t.room_num/t3.room_amount,0) as check_in_room_rate               --房间入住率
    ,coalesce((t3.room_amount-t.room_num)/t3.room_amount,0) as vacant_room_rate --房间空房率
    ,t.bed_num       --入住床位数
    ,t3.bed_amount    --楼层床位数
    ,coalesce(t.bed_num/t3.bed_amount,0) as check_in_bed_rate              --床位入住率
    ,coalesce((t3.bed_amount-t.bed_num)/t3.bed_amount,0) as vacant_bed_rate --床位空床率
    ,t.relatives_num --探访人数
    ,t.visit_time   --探访时长
    ,t.contract_time_1_num        --录入合同数量（1年期）
    ,t.contract_time_2_num        --录入合同数量（2年期）
    ,t.contract_time_3_num        --录入合同数量（3年期）
    ,t.contract_time_4_num        --录入合同数量（4年期）
    ,t.contract_time_5_num        --录入合同数量（5年期）
    ,t.contract_time_surpass_5_num --录入合同数量（5年以上）
    ,t.leave_num        --请假次数
    ,t.leave_time       --请假时长
    ,t.sick_leave_num   --病假次数
    ,t.casual_leave_num --事假次数
    ,t.other_leave_num  --其他假次数
    ,t.all_num          --总人数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as  area_id            --区域id
      ,sum(all_num)           as all_num             --所有人数
      ,sum(check_in_num) as check_in_num    --入住人数
      ,sum(countermand_num) as countermand_num --退住人数
      ,sum(relatives_num) as relatives_num --探访人数
      ,sum(contract_time_1_num) as contract_time_1_num        --录入合同数量（1年期）
      ,sum(contract_time_2_num) as contract_time_2_num        --录入合同数量（2年期）
      ,sum(contract_time_3_num) as contract_time_3_num        --录入合同数量（3年期）
      ,sum(contract_time_4_num) as contract_time_4_num        --录入合同数量（4年期）
      ,sum(contract_time_5_num) as contract_time_5_num        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num) as contract_time_surpass_5_num --录入合同数量（5年以上）
      ,sum(leave_num)        as leave_num --请假次数
      ,sum(leave_time)       as leave_time --请假时长
      ,sum(sick_leave_num)   as sick_leave_num     --病假次数
      ,sum(casual_leave_num) as casual_leave_num --事假次数
      ,sum(other_leave_num)  as other_leave_num   --其他假次数
      ,sum(visit_time)  as visit_time   --探访时长
      ,sum(room_num)    as room_num   --入住房间数
      ,sum(bed_num)  as bed_num   --入住床位数
  from db_dw_hr.clife_hr_dws_reside_manage_by_floor
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
)t 
left join
(
  select
       building_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
)t3 on t.building_id=t3.building_id


===========================
    clife_hr_dws_reside_manage_by_soyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_soyear partition(part_date)
select
     sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_year         --每年入住人数
    ,countermand_cnt_year    --每年退住人数
    ,room_num_year           --每年入住房间数
    ,bed_num_year            --每年入住床位数
    ,all_num_year            --每年总人数
    ,visit_num_year          --每年探访人数
    ,visit_time_year         --每年探访时长
    ,leave_num_year          --每年请假次数
    ,leave_time_year         --每年请假时长
    ,sick_leave_num_year     --每年病假次数
    ,casual_leave_num_year   --每年事假次数
    ,other_leave_num_year    --每年其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_year/all_num_year,0)           as check_in_rate_year      --每年入住率
    ,coalesce(countermand_cnt_year/all_num_year,0)      as countermand_rate_year   --每年退住率
    ,coalesce(room_num_year/room_amount,0)               as check_in_room_rate_year--每年房间入住率
    ,coalesce((room_amount-room_num_year)/room_amount,0) as vacant_room_rate_year  --每年房间空房率
    ,coalesce(bed_num_year/bed_amount,0)                 as check_in_bed_rate_year --每年床位入住率
    ,coalesce((bed_amount-bed_num_year)/bed_amount,0)    as vacant_bed_rate_year   --每年床位空床率	
    ,contract_time_1_num_year        --录入合同数量（1年期）
    ,contract_time_2_num_year        --录入合同数量（2年期）
    ,contract_time_3_num_year        --录入合同数量（3年期）
    ,contract_time_4_num_year        --录入合同数量（4年期）
    ,contract_time_5_num_year        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_year --录入合同数量（5年以上）
    ,refund_amount        --当年请假退费金额	
    ,leave_reducation_fee --当年请假减免费用
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_month)       as reside_cnt_year         --每年入住人数
      ,sum(countermand_cnt_month)  as countermand_cnt_year    --每年退住人数
      ,sum(room_num_month)         as room_num_year           --每年入住房间数
      ,sum(bed_num_month)          as bed_num_year            --每年入住床位数
      ,sum(all_num_month)          as all_num_year            --每年总人数
      ,sum(visit_num_month)        as visit_num_year          --每年探访人数
      ,sum(visit_time_month)       as visit_time_year         --每年探访时长
      ,sum(leave_num_month)        as leave_num_year          --每年请假次数
      ,sum(leave_time_month)       as leave_time_year         --每年请假时长
      ,sum(sick_leave_num_month)   as sick_leave_num_year     --每年病假次数
      ,sum(casual_leave_num_month) as casual_leave_num_year   --每年事假次数
      ,sum(other_leave_num_month)  as other_leave_num_year    --每年其他假次数
      ,sum(refund_amount)        as refund_amount        --当年请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当年请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_month)         contract_time_1_num_year        --录入合同数量（1年期）
      ,sum(contract_time_2_num_month)         contract_time_2_num_year        --录入合同数量（2年期）
      ,sum(contract_time_3_num_month)         contract_time_3_num_year        --录入合同数量（3年期）
      ,sum(contract_time_4_num_month)         contract_time_4_num_year        --录入合同数量（4年期）
      ,sum(contract_time_5_num_month)         contract_time_5_num_year        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num_month) contract_time_surpass_5_num_year--录入合同数量（5年以上）
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_reside_manage_by_somonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by sub_org_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_reside_manage_by_suborg
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_suborg partition(part_date)
select
     t.sub_org_id          --机构id
    ,t.sub_org_name        --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.check_in_num        --入住人数
    ,t.countermand_num     --退住人数
    ,coalesce(t.check_in_num/t.all_num,0) as  check_in_rate       --入住率
    ,coalesce(t.countermand_num/t.all_num,0) as countermand_rate  --退住率
    ,t.room_num       --入住房间数
    ,t3.room_amount    --楼层房间数
    ,coalesce(t.room_num/t3.room_amount,0) as check_in_room_rate               --房间入住率
    ,coalesce((t3.room_amount-t.room_num)/t3.room_amount,0) as vacant_room_rate --房间空房率
    ,t.bed_num       --入住床位数
    ,t3.bed_amount    --楼层床位数
    ,coalesce(t.bed_num/t3.bed_amount,0) as check_in_bed_rate              --床位入住率
    ,coalesce((t3.bed_amount-t.bed_num)/t3.bed_amount,0) as vacant_bed_rate --床位空床率
    ,t.relatives_num --探访人数
    ,t.visit_time   --探访时长
    ,t.contract_time_1_num        --录入合同数量（1年期）
    ,t.contract_time_2_num        --录入合同数量（2年期）
    ,t.contract_time_3_num        --录入合同数量（3年期）
    ,t.contract_time_4_num        --录入合同数量（4年期）
    ,t.contract_time_5_num        --录入合同数量（5年期）
    ,t.contract_time_surpass_5_num --录入合同数量（5年以上）
    ,t.leave_num        --请假次数
    ,t.leave_time       --请假时长
    ,t.sick_leave_num   --病假次数
    ,t.casual_leave_num --事假次数
    ,t.other_leave_num  --其他假次数
    ,t.all_num          --总人数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
   select
       sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as  area_id            --区域id
      ,sum(all_num)           as all_num             --所有人数
      ,sum(check_in_num)    as check_in_num    --入住人数
      ,sum(countermand_num) as countermand_num --退住人数
      ,sum(relatives_num) as relatives_num --探访人数
      ,sum(contract_time_1_num) as contract_time_1_num        --录入合同数量（1年期）
      ,sum(contract_time_2_num) as contract_time_2_num        --录入合同数量（2年期）
      ,sum(contract_time_3_num) as contract_time_3_num        --录入合同数量（3年期）
      ,sum(contract_time_4_num) as contract_time_4_num        --录入合同数量（4年期）
      ,sum(contract_time_5_num) as contract_time_5_num        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num) as contract_time_surpass_5_num --录入合同数量（5年以上）
      ,sum(leave_num)        as leave_num --请假次数
      ,sum(leave_time)       as leave_time --请假时长
      ,sum(sick_leave_num)   as sick_leave_num     --病假次数
      ,sum(casual_leave_num) as casual_leave_num --事假次数
      ,sum(other_leave_num)  as other_leave_num   --其他假次数
      ,sum(visit_time)  as visit_time   --探访时长
      ,sum(room_num)    as room_num   --入住房间数
      ,sum(bed_num)     as bed_num   --入住床位数
  from db_dw_hr.clife_hr_dws_reside_manage_by_nhome
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by sub_org_id
)t 
left join
(
  select
       b.sub_org_id
      ,count(a.room_id)  as room_amount 
      ,sum(a.bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room a
  left join db_dw_hr.clife_hr_dim_nursing_home b on a.nursing_homes_id = b.nursing_homes_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by b.sub_org_id
)t3 on t.sub_org_id=t3.sub_org_id


===========================
    clife_hr_dws_reside_manage_by_byear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_byear partition(part_date)
select
     building_id         --楼栋id
    ,nursin_homes_id     --养老院id
    ,nursing_homes_name  --养老院名称
    ,sub_org_id          --机构id
    ,sub_org_name        --机构名称
    ,org_id              --集团id
    ,org_name            --集团名称
    ,area_id             --区域id
    ,reside_cnt_year         --每年入住人数
    ,countermand_cnt_year    --每年退住人数
    ,room_num_year           --每年入住房间数
    ,bed_num_year            --每年入住床位数
    ,all_num_year            --每年总人数
    ,visit_num_year          --每年探访人数
    ,visit_time_year         --每年探访时长
    ,leave_num_year          --每年请假次数
    ,leave_time_year         --每年请假时长
    ,sick_leave_num_year     --每年病假次数
    ,casual_leave_num_year   --每年事假次数
    ,other_leave_num_year    --每年其他假次数
    ,room_amount --楼层房间数
    ,bed_amount  --楼层床位数
    ,coalesce(reside_cnt_year/all_num_year,0)           as check_in_rate_year      --每年入住率
    ,coalesce(countermand_cnt_year/all_num_year,0)      as countermand_rate_year   --每年退住率
    ,coalesce(room_num_year/room_amount,0)               as check_in_room_rate_year--每年房间入住率
    ,coalesce((room_amount-room_num_year)/room_amount,0) as vacant_room_rate_year  --每年房间空房率
    ,coalesce(bed_num_year/bed_amount,0)                 as check_in_bed_rate_year --每年床位入住率
    ,coalesce((bed_amount-bed_num_year)/bed_amount,0)    as vacant_bed_rate_year   --每年床位空床率	
    ,contract_time_1_num_year        --录入合同数量（1年期）
    ,contract_time_2_num_year        --录入合同数量（2年期）
    ,contract_time_3_num_year        --录入合同数量（3年期）
    ,contract_time_4_num_year        --录入合同数量（4年期）
    ,contract_time_5_num_year        --录入合同数量（5年期）
    ,contract_time_surpass_5_num_year --录入合同数量（5年以上）
    ,refund_amount        --当年请假退费金额	
    ,leave_reducation_fee --当年请假减免费用
    ,date_year --年份
    ,regexp_replace(date_sub(current_date(),1),'-','')
from
(
  select
       building_id         --楼栋id
      ,max(nursin_homes_id)    as nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as area_id             --区域id 
      ,sum(reside_cnt_month)       as reside_cnt_year         --每年入住人数
      ,sum(countermand_cnt_month)  as countermand_cnt_year    --每年退住人数
      ,sum(room_num_month)         as room_num_year           --每年入住房间数
      ,sum(bed_num_month)          as bed_num_year            --每年入住床位数
      ,sum(all_num_month)          as all_num_year            --每年总人数
      ,sum(visit_num_month)        as visit_num_year          --每年探访人数
      ,sum(visit_time_month)       as visit_time_year         --每年探访时长
      ,sum(leave_num_month)        as leave_num_year          --每年请假次数
      ,sum(leave_time_month)       as leave_time_year         --每年请假时长
      ,sum(sick_leave_num_month)   as sick_leave_num_year     --每年病假次数
      ,sum(casual_leave_num_month) as casual_leave_num_year   --每年事假次数
      ,sum(other_leave_num_month)  as other_leave_num_year    --每年其他假次数
      ,sum(refund_amount)        as refund_amount        --当年请假退费金额
      ,sum(leave_reducation_fee) as leave_reducation_fee --当年请假减免费用
      ,max(room_amount) as room_amount --楼层房间数
      ,max(bed_amount)  as bed_amount  --楼层床位数	
      ,sum(contract_time_1_num_month)          as contract_time_1_num_year        
      ,sum(contract_time_2_num_month)          as contract_time_2_num_year        
      ,sum(contract_time_3_num_month)          as contract_time_3_num_year        
      ,sum(contract_time_4_num_month)          as contract_time_4_num_year        
      ,sum(contract_time_5_num_month)          as contract_time_5_num_year        
      ,sum(contract_time_surpass_5_num_month)  as contract_time_surpass_5_num_year
      ,substr(date_month,1,4) as date_year --年份
  from db_dw_hr.clife_hr_dws_reside_manage_by_bmonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by building_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_reside_manage_by_nhome
===========================
insert overwrite table db_dw_hr.clife_hr_dws_reside_manage_by_nhome partition(part_date)
select
     t.nursin_homes_id     --养老院id
    ,t.nursing_homes_name  --养老院名称
    ,t.sub_org_id          --机构id
    ,t.sub_org_name        --机构名称
    ,t.org_id              --集团id
    ,t.org_name            --集团名称
    ,t.area_id             --区域id
    ,t.check_in_num        --入住人数
    ,t.countermand_num     --退住人数
    ,coalesce(t.check_in_num/t.all_num,0) as  check_in_rate       --入住率
    ,coalesce(t.countermand_num/t.all_num,0) as countermand_rate  --退住率
    ,t.room_num       --入住房间数
    ,t3.room_amount    --楼层房间数
    ,coalesce(t.room_num/t3.room_amount,0) as check_in_room_rate               --房间入住率
    ,coalesce((t3.room_amount-t.room_num)/t3.room_amount,0) as vacant_room_rate --房间空房率
    ,t.bed_num       --入住床位数
    ,t3.bed_amount    --楼层床位数
    ,coalesce(t.bed_num/t3.bed_amount,0) as check_in_bed_rate              --床位入住率
    ,coalesce((t3.bed_amount-t.bed_num)/t3.bed_amount,0) as vacant_bed_rate --床位空床率
    ,t.relatives_num --探访人数
    ,t.visit_time   --探访时长
    ,t.contract_time_1_num        --录入合同数量（1年期）
    ,t.contract_time_2_num        --录入合同数量（2年期）
    ,t.contract_time_3_num        --录入合同数量（3年期）
    ,t.contract_time_4_num        --录入合同数量（4年期）
    ,t.contract_time_5_num        --录入合同数量（5年期）
    ,t.contract_time_surpass_5_num --录入合同数量（5年以上）
    ,t.leave_num        --请假次数
    ,t.leave_time       --请假时长
    ,t.sick_leave_num   --病假次数
    ,t.casual_leave_num --事假次数
    ,t.other_leave_num  --其他假次数
    ,t.all_num          --总人数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
   select
       nursin_homes_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name  --养老院名称
      ,max(sub_org_id)         as sub_org_id          --机构id
      ,max(sub_org_name)       as sub_org_name        --机构名称
      ,max(org_id)             as org_id              --集团id
      ,max(org_name)           as org_name            --集团名称
      ,max(area_id)            as  area_id            --区域id
      ,sum(all_num)           as all_num             --所有人数
      ,sum(check_in_num)    as check_in_num    --入住人数
      ,sum(countermand_num) as countermand_num --退住人数
      ,sum(relatives_num) as relatives_num --探访人数
      ,sum(contract_time_1_num) as contract_time_1_num        --录入合同数量（1年期）
      ,sum(contract_time_2_num) as contract_time_2_num        --录入合同数量（2年期）
      ,sum(contract_time_3_num) as contract_time_3_num        --录入合同数量（3年期）
      ,sum(contract_time_4_num) as contract_time_4_num        --录入合同数量（4年期）
      ,sum(contract_time_5_num) as contract_time_5_num        --录入合同数量（5年期）
      ,sum(contract_time_surpass_5_num) as contract_time_surpass_5_num --录入合同数量（5年以上）
      ,sum(leave_num)        as leave_num --请假次数
      ,sum(leave_time)       as leave_time --请假时长
      ,sum(sick_leave_num)   as sick_leave_num     --病假次数
      ,sum(casual_leave_num) as casual_leave_num --事假次数
      ,sum(other_leave_num)  as other_leave_num   --其他假次数
      ,sum(visit_time)  as visit_time   --探访时长
      ,sum(room_num)    as room_num   --入住房间数
      ,sum(bed_num)     as bed_num   --入住床位数
  from db_dw_hr.clife_hr_dws_reside_manage_by_building
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
)t 
left join
(
  select
       nursing_homes_id
      ,count(room_id)  as room_amount 
      ,sum(bed_amount) as bed_amount
  from db_dw_hr.clife_hr_dim_location_room
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursing_homes_id
)t3 on t.nursin_homes_id=t3.nursing_homes_id


===========================
    clife_hr_dws_recharge_consumption_by_ityear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_ityear partition(part_date)
select
     fee_iteam_id
    ,sub_org_id
    ,sub_org_name                  --机构名称
    ,org_id                        --集团id
    ,org_name                      --集团名称
    ,fee_iteam_name                --费用项目名称
    ,fee_type_id                   --费用类型id
    ,fee_type_name                 --费用类型名称
    ,iteam_old_man_cnt_year      --账户老人数
    ,iteam_member_cnt_year       --账户会员数
    ,consume_amount_year         --每年账户消费金额
    ,consume_num_year            --每年账户消费次数
    ,member_consume_amount_year  --每年会员消费金额
    ,member_consume_num_year     --每年会员消费次数
    ,coalesce(consume_amount_year/iteam_old_man_cnt_year,0)       as consume_avg_year         --人均消费金额
    ,coalesce(consume_num_year/iteam_old_man_cnt_year,0)          as consume_cnt_avg_year  --人均消费次数
    ,coalesce(member_consume_amount_year/iteam_member_cnt_year,0) as member_consume_avg_year         --会员人均消费金额
    ,coalesce(member_consume_num_year/iteam_member_cnt_year,0)    as member_consume_cnt_avg_year  --会员人均消费次数
    ,date_year                     --年份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       fee_iteam_id
      ,sub_org_id
      ,max(sub_org_name)              as sub_org_name                 --机构名称
      ,max(org_id)                    as org_id                       --集团id
      ,max(org_name)                  as org_name                     --集团名称
      ,max(fee_iteam_name)            as fee_iteam_name               --费用项目名称
      ,max(fee_type_id)               as fee_type_id                  --费用类型id
      ,max(fee_type_name)             as fee_type_name                --费用类型名称
      ,sum(iteam_old_man_cnt_month)     as iteam_old_man_cnt_year      --账户老人数
      ,sum(iteam_member_cnt_month)      as iteam_member_cnt_year       --账户会员数
      ,sum(consume_amount_month)        as consume_amount_year         --每年账户消费金额
      ,sum(consume_num_month)           as consume_num_year            --每年账户消费次数
      ,sum(member_consume_amount_month) as member_consume_amount_year  --每年会员消费金额
      ,sum(member_consume_num_month)    as member_consume_num_year     --每年会员消费次数
      ,substr(date_month,1,4)           as date_year           --年份
  from db_dw_hr.clife_hr_dws_recharge_consumption_by_itmonth
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by fee_iteam_id
          ,sub_org_id
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_recharge_consumption_by_itmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_itmonth partition(part_date)
select
     fee_iteam_id
    ,sub_org_id
    ,sub_org_name                  --机构名称
    ,org_id                        --集团id
    ,org_name                      --集团名称
    ,fee_iteam_name                --费用项目名称
    ,fee_type_id                   --费用类型id
    ,fee_type_name                 --费用类型名称
    ,iteam_old_man_cnt_month      --账户老人数
    ,iteam_member_cnt_month       --账户会员数
    ,consume_amount_month         --每月账户消费金额
    ,consume_num_month            --每月账户消费次数
    ,member_consume_amount_month  --每月会员消费金额
    ,member_consume_num_month     --每月会员消费次数
    ,coalesce(consume_amount_month/iteam_old_man_cnt_month,0)       as consume_avg_month         --人均消费金额
    ,coalesce(consume_num_month/iteam_old_man_cnt_month,0)          as consume_cnt_avg_month  --人均消费次数
    ,coalesce(member_consume_amount_month/iteam_member_cnt_month,0) as member_consume_avg_month         --会员人均消费金额
    ,coalesce(member_consume_num_month/iteam_member_cnt_month,0)    as member_consume_cnt_avg_month  --会员人均消费次数
    ,date_month                     --月份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       fee_iteam_id
      ,sub_org_id
      ,max(sub_org_name)              as sub_org_name                 --机构名称
      ,max(org_id)                    as org_id                       --集团id
      ,max(org_name)                  as org_name                     --集团名称
      ,max(fee_iteam_name)            as fee_iteam_name               --费用项目名称
      ,max(fee_type_id)               as fee_type_id                  --费用类型id
      ,max(fee_type_name)             as fee_type_name                --费用类型名称
      ,sum(iteam_old_man_cnt)         as iteam_old_man_cnt_month      --账户老人数
      ,sum(iteam_member_cnt)          as iteam_member_cnt_month       --账户会员数
      ,sum(consume_amount_day)        as consume_amount_month         --每月账户消费金额
      ,sum(consume_num_day)           as consume_num_month            --每月账户消费次数
      ,sum(member_consume_amount_day) as member_consume_amount_month  --每月会员消费金额
      ,sum(member_consume_num_day)    as member_consume_num_month     --每月会员消费次数
      ,substr(date_time,1,7)          as date_month           --月份
  from db_dw_hr.clife_hr_dws_recharge_consumption_by_itday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by fee_iteam_id
          ,sub_org_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_recharge_consumption_by_itday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_itday partition(part_date)
select
     fee_iteam_id
    ,sub_org_id
    ,sub_org_name                  --机构名称
    ,org_id                        --集团id
    ,org_name                      --集团名称
    ,fee_iteam_name                --费用项目名称
    ,fee_type_id                   --费用类型id
    ,fee_type_name                 --费用类型名称
    ,iteam_old_man_cnt             --账户老人数
    ,iteam_member_cnt              --账户会员数
    ,consume_amount_day            --每天账户消费金额
    ,consume_num_day               --每天账户消费次数
    ,member_consume_amount_day     --每天会员消费金额
    ,member_consume_num_day        --每天会员消费次数
    ,coalesce(consume_amount_day/iteam_old_man_cnt,0)       as consume_avg --人均消费金额
    ,coalesce(consume_num_day/iteam_old_man_cnt,0)          as consume_cnt_avg  --人均消费次数
    ,coalesce(member_consume_amount_day/iteam_member_cnt,0) as member_consume_avg --会员人均消费金额
    ,coalesce(member_consume_num_day/iteam_member_cnt,0)    as member_consume_cnt_avg  --会员人均消费次数
    ,date_time                     --日期
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       fee_iteam_id
      ,sub_org_id
      ,max(sub_org_name)        as sub_org_name      --机构名称
      ,max(org_id)              as org_id            --集团id
      ,max(org_name)            as org_name          --集团名称
      ,max(fee_iteam_name)      as fee_iteam_name    --费用项目名称
      ,max(fee_type_id)         as fee_type_id       --费用类型id
      ,max(fee_type_name)       as fee_type_name     --费用类型名称
      ,sum(member_consume_num_day)     as member_consume_num_day       --每天会员消费次数
      ,sum(member_consume_amount_day)  as member_consume_amount_day    --每天会员消费金额
      ,sum(consume_num_day)            as consume_num_day               --每天账户消费次数
      ,sum(consume_amount_day)         as consume_amount_day            --每天账户消费金额
      ,sum(iteam_member_cnt)         as iteam_member_cnt            --账户会员数
      ,sum(iteam_old_man_cnt)        as iteam_old_man_cnt           --账户老人数
      ,date_time
  from
  (
    select
         fee_iteam_id
        ,sub_org_id
        ,max(sub_org_name)        as sub_org_name      --机构名称
        ,max(org_id)              as org_id            --集团id
        ,max(org_name)            as org_name          --集团名称
        ,max(fee_iteam_name)      as fee_iteam_name    --费用项目名称
        ,max(fee_type_id)         as fee_type_id       --费用类型id
        ,max(fee_type_name)       as fee_type_name     --费用类型名称
        ,count(1)    as member_consume_num_day         --每天会员消费次数
        ,sum(amount) as member_consume_amount_day      --每天会员消费金额
        ,0 as consume_num_day         --每天账户消费次数
        ,0 as consume_amount_day      --每天账户消费金额
        ,count(distinct member_id)  as iteam_member_cnt   --账户会员数
        ,0 as iteam_old_man_cnt  --账户老人数
        ,substr(recharge_consumption_time,1,10) as date_time
     from db_dw_hr.clife_hr_dwd_recharge_consumption_info
     where part_date = regexp_replace(date_sub(current_date(),1),'-','')
       and behavior_type = '会员消费'
     group by fee_iteam_id
             ,sub_org_id
             ,substr(recharge_consumption_time,1,10) 
    union all
    select
         fee_iteam_id
        ,sub_org_id
        ,max(sub_org_name)        as sub_org_name      --机构名称
        ,max(org_id)              as org_id            --集团id
        ,max(org_name)            as org_name          --集团名称
        ,max(fee_iteam_name)      as fee_iteam_name    --费用项目名称
        ,max(fee_type_id)         as fee_type_id       --费用类型id
        ,max(fee_type_name)       as fee_type_name     --费用类型名称
        ,0 as member_consume_num_day         --每天会员消费次数
        ,0 as member_consume_amount_day      --每天会员消费金额
        ,count(1)    as consume_num_day         --每天账户消费次数
        ,sum(amount) as consume_amount_day      --每天账户消费金额
        ,0 as iteam_member_cnt   --账户会员数
        ,count(distinct old_man_id) as iteam_old_man_cnt  --账户老人数
        ,substr(recharge_consumption_time,1,10) as date_time
    from db_dw_hr.clife_hr_dwd_recharge_consumption_info
    where part_date = regexp_replace(date_sub(current_date(),1),'-','')
      and behavior_type = '账户消费'
    group by fee_iteam_id
            ,sub_org_id
            ,substr(recharge_consumption_time,1,10)
  )a
  group by fee_iteam_id
          ,sub_org_id
          ,date_time
)t


===========================
    clife_hr_dws_recharge_consumption_by_iteam
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_iteam partition(part_date)
select
     fee_iteam_id          --费用项目id
    ,sub_org_id            --养老机构id
    ,sub_org_name          --机构名称
    ,org_id                --集团id
    ,org_name              --集团名称
    ,fee_iteam_name        --费用项目名称
    ,fee_type_id           --费用类型id
    ,fee_type_name         --费用类型名称
    ,iteam_old_man_cnt     --老人数
    ,iteam_member_cnt      --会员数
    ,consume_amount    --花费总金额
    ,consume_num       --花费总次数
    ,member_consume_amount --会员消费总金额
    ,member_consume_num    --会员消费总次数
    ,coalesce(consume_amount/iteam_old_man_cnt,0) as consume_avg --人均消费金额
    ,coalesce(consume_num/iteam_old_man_cnt,0)    as consume_cnt_avg  --人均消费次数
    ,coalesce(member_consume_amount/iteam_member_cnt,0) as member_consume_avg --会员人均消费金额
    ,coalesce(member_consume_num/iteam_member_cnt,0)    as member_consume_cnt_avg  --会员人均消费次数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       fee_iteam_id        --费用项目id
      ,sub_org_id          --养老机构id
      ,max(sub_org_name)        as sub_org_name      --机构名称
      ,max(org_id)              as org_id            --集团id
      ,max(org_name)            as org_name          --集团名称
      ,max(fee_iteam_name)      as fee_iteam_name    --费用项目名称
      ,max(fee_type_id)         as fee_type_id       --费用类型id
      ,max(fee_type_name)       as fee_type_name     --费用类型名称
      ,count(distinct old_man_id) as iteam_old_man_cnt  --老人数
      ,count(distinct member_id)  as iteam_member_cnt   --会员数
      ,sum(consume_amount) as consume_amount --花费总金额
      ,sum(consume_num)    as consume_num  --花费总次数
      ,sum(member_consume_amount) as member_consume_amount --会员消费总金额
      ,sum(member_consume_num)    as member_consume_num    --会员消费总次数
  from db_dw_hr.clife_hr_dwm_recharge_consumption_aggregation
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by fee_iteam_id
          ,sub_org_id
)t


===========================
    clife_hr_dws_recharge_consumption_by_anso
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_anso partition(part_date)
select
     account_name            --账户名称
    ,sub_org_id              --机构id
    ,sub_org_name            --机构名称
    ,org_id                  --集团id
    ,org_name                --集团名称
    ,account_old_man_cnt     --账户老人数
    ,account_member_cnt      --账户会员数
    ,check_in_payment_num    --入住缴费次数
    ,check_in_payment_amount --入住缴费金额
    ,recharge_num            --账户充值次数
    ,recharge_amount         --账户充值金额
    ,member_recharge_num     --会员充值次数
    ,member_recharge_amount  --会员充值金额
    ,consume_num             --账户消费次数
    ,consume_amount          --账户消费金额
    ,member_consume_num      --会员消费次数
    ,member_consume_amount   --会员消费金额
    ,countermand_amount      --退住退费金额
    ,coalesce(recharge_amount/recharge_old_man_num,0) as recharge_avg --人均充值金额
    ,coalesce(member_recharge_amount/recharge_member_man_num,0) as member_recharge_avg  --会员人均充值金额
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       b.account_name  --账户名称
      ,a.sub_org_id  --机构id
      ,max(a.sub_org_name)       as sub_org_name            --机构名称
      ,max(a.org_id)             as org_id                  --集团id
      ,max(a.org_name)           as org_name                --集团名称
      ,count(distinct a.old_man_id) as account_old_man_cnt  --账户老人数
      ,count(distinct a.member_id)  as account_member_cnt   --账户会员数
      ,sum(case when a.behavior_type='入住缴费' then 1 else 0 end)      as check_in_payment_num      --入住缴费次数
      ,sum(case when a.behavior_type='入住缴费' then a.amount else 0 end) as check_in_payment_amount   --入住缴费金额
      ,sum(case when a.behavior_type='账户充值' then 1 else 0 end)      as recharge_num              --账户充值次数
      ,sum(case when a.behavior_type='账户充值' then a.amount else 0 end) as recharge_amount           --账户充值金额
      ,sum(case when a.behavior_type='会员充值' then 1 else 0 end)      as member_recharge_num       --会员充值次数
      ,sum(case when a.behavior_type='会员充值' then a.amount else 0 end) as member_recharge_amount    --会员充值金额
      ,sum(case when a.behavior_type='账户消费' then 1 else 0 end)      as consume_num               --账户消费次数
      ,sum(case when a.behavior_type='账户消费' then a.amount else 0 end) as consume_amount            --账户消费金额
      ,sum(case when a.behavior_type='会员消费' then 1 else 0 end)      as member_consume_num        --会员消费金额
      ,sum(case when a.behavior_type='会员消费' then a.amount else 0 end) as member_consume_amount     --会员消费次数
      ,sum(case when a.behavior_type='退住退费' then a.amount else 0 end) as countermand_amount        --退住退费金额
      ,count(distinct (case when a.behavior_type='账户充值' then a.old_man_id else null end)) as recharge_old_man_num   --账户充值老人数
      ,count(distinct (case when a.behavior_type='会员充值' then a.member_id else null end)) as recharge_member_man_num   --账户充值老人数
  from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
  left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  group by b.account_name
          ,a.sub_org_id
)t


===========================
    clife_hr_dws_recharge_consumption_by_asyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_asyear partition(part_date)
select
     sub_org_id              --机构id
    ,account_name            --账户名称
    ,sub_org_name            --机构名称
    ,org_id                  --集团id
    ,org_name                --集团名称
    ,account_old_man_cnt_year          --账户老人数
    ,account_member_cnt_year           --账户会员数
    ,check_in_payment_num_year     --每月入住缴费次数
    ,check_in_payment_amount_year  --每月入住缴费金额
    ,recharge_num_year             --每月账户充值次数
    ,recharge_amount_year          --每月账户充值金额
    ,member_recharge_num_year      --每月会员充值次数
    ,member_recharge_amount_year   --每月会员充值金额
    ,consume_num_year              --每月账户消费次数
    ,consume_amount_year           --每月账户消费金额
    ,member_consume_num_year       --每月会员消费次数
    ,member_consume_amount_year    --每月会员消费金额
    ,countermand_amount_year       --每月退住退费金额
    ,coalesce(recharge_amount_year/recharge_old_man_cnt,0) as recharge_avg_year --人均充值金额
    ,coalesce(member_recharge_amount_year/recharge_member_cnt,0) as  member_recharge_avg_year  --会员人均充值金额
    ,date_year
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       sub_org_id              --机构id
      ,account_name            --账户名称
      ,max(sub_org_name) as   sub_org_name   --机构名称
      ,max(org_id)       as   org_id         --集团id
      ,max(org_name)     as   org_name       --集团名称
      ,sum(account_old_man_cnt_year)          as account_old_man_cnt_year          --账户老人数
      ,sum(account_member_cnt_year)           as account_member_cnt_year           --账户会员数
      ,sum(check_in_payment_num_year)     as check_in_payment_num_year     --每月入住缴费次数
      ,sum(check_in_payment_amount_year)  as check_in_payment_amount_year  --每月入住缴费金额
      ,sum(recharge_num_year)             as recharge_num_year             --每月账户充值次数
      ,sum(recharge_amount_year)          as recharge_amount_year          --每月账户充值金额
      ,sum(member_recharge_num_year)      as member_recharge_num_year      --每月会员充值次数
      ,sum(member_recharge_amount_year)   as member_recharge_amount_year   --每月会员充值金额
      ,sum(consume_num_year)              as consume_num_year              --每月账户消费次数
      ,sum(consume_amount_year)           as consume_amount_year           --每月账户消费金额
      ,sum(member_consume_num_year)       as member_consume_num_year       --每月会员消费次数
      ,sum(member_consume_amount_year)    as member_consume_amount_year    --每月会员消费金额
      ,sum(countermand_amount_year)       as countermand_amount_year       --每月退住退费金额
      ,sum(recharge_old_man_cnt)           as recharge_old_man_cnt         --基础充值老人数
      ,sum(recharge_member_cnt)            as recharge_member_cnt          --会员充值老人数
      ,date_year
  from
  (
    select
         sub_org_id              --机构id
        ,account_name            --账户名称
        ,max(sub_org_name) as   sub_org_name   --机构名称
        ,max(org_id)       as   org_id         --集团id
        ,max(org_name)     as   org_name       --集团名称
        ,sum(account_old_man_cnt_month)          as account_old_man_cnt_year            --账户老人数
        ,sum(account_member_cnt_month)           as account_member_cnt_year             --账户会员数
        ,sum(check_in_payment_num_month)     as check_in_payment_num_year     --每月入住缴费次数
        ,sum(check_in_payment_amount_month)  as check_in_payment_amount_year  --每月入住缴费金额
        ,sum(recharge_num_month)             as recharge_num_year             --每月账户充值次数
        ,sum(recharge_amount_month)          as recharge_amount_year          --每月账户充值金额
        ,sum(member_recharge_num_month)      as member_recharge_num_year      --每月会员充值次数
        ,sum(member_recharge_amount_month)   as member_recharge_amount_year   --每月会员充值金额
        ,sum(consume_num_month)              as consume_num_year              --每月账户消费次数
        ,sum(consume_amount_month)           as consume_amount_year           --每月账户消费金额
        ,sum(member_consume_num_month)       as member_consume_num_year       --每月会员消费次数
        ,sum(member_consume_amount_month)    as member_consume_amount_year    --每月会员消费金额
        ,sum(countermand_amount_month)       as countermand_amount_year       --每月退住退费金额
        ,0 as recharge_old_man_cnt  --基础充值老人数
        ,0 as recharge_member_cnt   --会员充值老人数
        ,substr(date_month,1,4) as date_year --月份
    from db_dw_hr.clife_hr_dws_recharge_consumption_by_asmonth
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by  sub_org_id  
             ,account_name
             ,substr(date_month,1,4)
   union all
   select
        a.sub_org_id
       ,b.account_name
       ,max(a.sub_org_name) as   sub_org_name   --机构名称
       ,max(a.org_id)       as   org_id         --集团id
       ,max(a.org_name)     as   org_name       --集团名称
       ,0 as account_old_man_cnt_year          --账户老人数	   
       ,0 as account_member_cnt_year           --账户会员数	   
       ,0 as check_in_payment_num_year    --每月入住缴费次数	   
       ,0 as check_in_payment_amount_year  --每月入住缴费金额	   
       ,0 as recharge_num_year             --每月账户充值次数	   
       ,0 as recharge_amount_year          --每月账户充值金额	   
       ,0 as member_recharge_num_year      --每月会员充值次数	   
       ,0 as member_recharge_amount_year   --每月会员充值金额	   
       ,0 as consume_num_year              --每月账户消费次数	   
       ,0 as consume_amount_year           --每月账户消费金额	   
       ,0 as member_consume_num_year       --每月会员消费次数	   
       ,0 as member_consume_amount_year    --每月会员消费金额	   
       ,0 as countermand_amount_year       --每月退住退费金额
       ,count(distinct a.old_man_id) as recharge_old_man_cnt  --基础充值老人数
       ,0 as recharge_member_cnt  --会员充值老人数
       ,substr(a.recharge_consumption_time,1,4) as date_year
   from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
   left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
   where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
     and a.behavior_type = '账户充值'
   group by b.account_name
           ,a.sub_org_id
           ,substr(a.recharge_consumption_time,1,4)
   union all
   select
        a.sub_org_id
       ,b.account_name
       ,max(a.sub_org_name) as   sub_org_name   --机构名称
       ,max(a.org_id)       as   org_id         --集团id
       ,max(a.org_name)     as   org_name       --集团名称
       ,0 as account_old_man_cnt_year          --账户老人数	   
       ,0 as account_member_cnt_year           --账户会员数	   
       ,0 as check_in_payment_num_year     --每月入住缴费次数	   
       ,0 as check_in_payment_amount_year  --每月入住缴费金额	   
       ,0 as recharge_num_year             --每月账户充值次数	   
       ,0 as recharge_amount_year          --每月账户充值金额	   
       ,0 as member_recharge_num_year      --每月会员充值次数	   
       ,0 as member_recharge_amount_year   --每月会员充值金额	   
       ,0 as consume_num_year              --每月账户消费次数	   
       ,0 as consume_amount_year           --每月账户消费金额	   
       ,0 as member_consume_num_year       --每月会员消费次数	   
       ,0 as member_consume_amount_year    --每月会员消费金额	   
       ,0 as countermand_amount_year       --每月退住退费金额
       ,0 as recharge_old_man_cnt  --基础充值老人数
       ,count(distinct a.old_man_id) as recharge_member_cnt  --会员充值老人数
       ,substr(a.recharge_consumption_time,1,4) as date_year
   from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
   left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
   where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
     and a.behavior_type = '会员充值'
   group by b.account_name
           ,a.sub_org_id
           ,substr(a.recharge_consumption_time,1,4)
  )a
  group by sub_org_id  
          ,account_name
          ,date_year
)t


===========================
    clife_hr_dws_recharge_consumption_by_asmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_recharge_consumption_by_asmonth partition(part_date)
select
     sub_org_id              --机构id
    ,account_name            --账户名称
    ,sub_org_name            --机构名称
    ,org_id                  --集团id
    ,org_name                --集团名称
    ,account_old_man_cnt_month          --账户老人数
    ,account_member_cnt_month           --账户会员数
    ,check_in_payment_num_month     --每月入住缴费次数
    ,check_in_payment_amount_month  --每月入住缴费金额
    ,recharge_num_month             --每月账户充值次数
    ,recharge_amount_month          --每月账户充值金额
    ,member_recharge_num_month      --每月会员充值次数
    ,member_recharge_amount_month   --每月会员充值金额
    ,consume_num_month              --每月账户消费次数
    ,consume_amount_month           --每月账户消费金额
    ,member_consume_num_month       --每月会员消费次数
    ,member_consume_amount_month    --每月会员消费金额
    ,countermand_amount_month       --每月退住退费金额
    ,coalesce(recharge_amount_month/recharge_old_man_cnt,0) as recharge_avg_month --人均充值金额
    ,coalesce(member_recharge_amount_month/recharge_member_cnt,0) as  member_recharge_avg_month  --会员人均充值金额
    ,date_month
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       sub_org_id              --机构id
      ,account_name            --账户名称
      ,max(sub_org_name) as   sub_org_name   --机构名称
      ,max(org_id)       as   org_id         --集团id
      ,max(org_name)     as   org_name       --集团名称
      ,sum(account_old_man_cnt_month)          as account_old_man_cnt_month          --账户老人数
      ,sum(account_member_cnt_month)           as account_member_cnt_month           --账户会员数
      ,sum(check_in_payment_num_month)     as check_in_payment_num_month     --每月入住缴费次数
      ,sum(check_in_payment_amount_month)  as check_in_payment_amount_month  --每月入住缴费金额
      ,sum(recharge_num_month)             as recharge_num_month             --每月账户充值次数
      ,sum(recharge_amount_month)          as recharge_amount_month          --每月账户充值金额
      ,sum(member_recharge_num_month)      as member_recharge_num_month      --每月会员充值次数
      ,sum(member_recharge_amount_month)   as member_recharge_amount_month   --每月会员充值金额
      ,sum(consume_num_month)              as consume_num_month              --每月账户消费次数
      ,sum(consume_amount_month)           as consume_amount_month           --每月账户消费金额
      ,sum(member_consume_num_month)       as member_consume_num_month       --每月会员消费次数
      ,sum(member_consume_amount_month)    as member_consume_amount_month    --每月会员消费金额
      ,sum(countermand_amount_month)       as countermand_amount_month       --每月退住退费金额
      ,sum(recharge_old_man_cnt)         as recharge_old_man_cnt         --基础充值老人数
      ,sum(recharge_member_cnt)          as recharge_member_cnt          --会员充值老人数
      ,date_month
  from
  (
    select
         sub_org_id              --机构id
        ,account_name            --账户名称
        ,max(sub_org_name) as   sub_org_name   --机构名称
        ,max(org_id)       as   org_id         --集团id
        ,max(org_name)     as   org_name       --集团名称
        ,sum(account_old_man_cnt)          as account_old_man_cnt_month            --账户老人数
        ,sum(account_member_cnt)           as account_member_cnt_month             --账户会员数
        ,sum(check_in_payment_num_day)     as check_in_payment_num_month     --每月入住缴费次数
        ,sum(check_in_payment_amount_day)  as check_in_payment_amount_month  --每月入住缴费金额
        ,sum(recharge_num_day)             as recharge_num_month             --每月账户充值次数
        ,sum(recharge_amount_day)          as recharge_amount_month          --每月账户充值金额
        ,sum(member_recharge_num_day)      as member_recharge_num_month      --每月会员充值次数
        ,sum(member_recharge_amount_day)   as member_recharge_amount_month   --每月会员充值金额
        ,sum(consume_num_day)              as consume_num_month              --每月账户消费次数
        ,sum(consume_amount_day)           as consume_amount_month           --每月账户消费金额
        ,sum(member_consume_num_day)       as member_consume_num_month       --每月会员消费次数
        ,sum(member_consume_amount_day)    as member_consume_amount_month    --每月会员消费金额
        ,sum(countermand_amount_day)       as countermand_amount_month       --每月退住退费金额
        ,0 as recharge_old_man_cnt  --基础充值老人数
        ,0 as recharge_member_cnt   --会员充值老人数
        ,substr(date_time,1,7) as date_month --月份
    from db_dw_hr.clife_hr_dws_recharge_consumption_by_asday
    where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    group by  sub_org_id  
             ,account_name
             ,substr(date_time,1,7)
   union all
   select
        a.sub_org_id
       ,b.account_name
       ,max(a.sub_org_name) as   sub_org_name   --机构名称
       ,max(a.org_id)       as   org_id         --集团id
       ,max(a.org_name)     as   org_name       --集团名称
       ,0 as account_old_man_cnt_month          --账户老人数	   
       ,0 as account_member_cnt_month           --账户会员数	   
       ,0 as check_in_payment_num_month    --每月入住缴费次数	   
       ,0 as check_in_payment_amount_month  --每月入住缴费金额	   
       ,0 as recharge_num_month             --每月账户充值次数	   
       ,0 as recharge_amount_month          --每月账户充值金额	   
       ,0 as member_recharge_num_month      --每月会员充值次数	   
       ,0 as member_recharge_amount_month   --每月会员充值金额	   
       ,0 as consume_num_month              --每月账户消费次数	   
       ,0 as consume_amount_month           --每月账户消费金额	   
       ,0 as member_consume_num_month       --每月会员消费次数	   
       ,0 as member_consume_amount_month    --每月会员消费金额	   
       ,0 as countermand_amount_month       --每月退住退费金额
       ,count(distinct a.old_man_id) as recharge_old_man_cnt  --基础充值老人数
       ,0 as recharge_member_cnt  --会员充值老人数
       ,substr(a.recharge_consumption_time,1,7) as date_month
   from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
   left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
   where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
     and a.behavior_type = '账户充值'
   group by b.account_name
           ,a.sub_org_id
           ,substr(a.recharge_consumption_time,1,7)
   union all
   select
        a.sub_org_id
       ,b.account_name
       ,max(a.sub_org_name) as   sub_org_name   --机构名称
       ,max(a.org_id)       as   org_id         --集团id
       ,max(a.org_name)     as   org_name       --集团名称
       ,0 as account_old_man_cnt_month          --账户老人数	   
       ,0 as account_member_cnt_month           --账户会员数	   
       ,0 as check_in_payment_num_month     --每月入住缴费次数	   
       ,0 as check_in_payment_amount_month  --每月入住缴费金额	   
       ,0 as recharge_num_month             --每月账户充值次数	   
       ,0 as recharge_amount_month          --每月账户充值金额	   
       ,0 as member_recharge_num_month      --每月会员充值次数	   
       ,0 as member_recharge_amount_month   --每月会员充值金额	   
       ,0 as consume_num_month              --每月账户消费次数	   
       ,0 as consume_amount_month           --每月账户消费金额	   
       ,0 as member_consume_num_month       --每月会员消费次数	   
       ,0 as member_consume_amount_month    --每月会员消费金额	   
       ,0 as countermand_amount_month       --每月退住退费金额
       ,0 as recharge_old_man_cnt  --基础充值老人数
       ,count(distinct a.old_man_id) as recharge_member_cnt  --会员充值老人数
       ,substr(a.recharge_consumption_time,1,7) as date_month
   from db_dw_hr.clife_hr_dwd_recharge_consumption_info a
   left join db_dw_hr.clife_hr_dim_account b on a.account_id = b.account_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
   where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')
     and a.behavior_type = '会员充值'
   group by b.account_name
           ,a.sub_org_id
           ,substr(a.recharge_consumption_time,1,7)
  )a
  group by sub_org_id  
          ,account_name
          ,date_month
)t


===========================
    clife_hr_dws_coupon_manage_by_nhday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_coupon_manage_by_nhday partition(part_date)
select
     nurse_home_id
    ,max(nursing_homes_name) as nursing_homes_name --养老院名称
    ,max(sub_org_id)         as sub_org_id         --机构id
    ,max(sub_org_name)       as sub_org_name       --机构名称
    ,max(org_id)             as org_id             --集团id
    ,max(org_name)           as org_name           --集团名称
    ,max(area_id)            as area_id            --区域id
    ,sum(deduction_coupon_use_num_day)as deduction_coupon_use_num_day --每天抵扣优惠券使用数量
    ,sum(discount_coupon_use_num_day) as discount_coupon_use_num_day  --每天折扣优惠券使用数量
    ,sum(cash_coupon_use_num_day)     as cash_coupon_use_num_day      --每天现金优惠券使用数量
    ,sum(deduction_coupon_cost_day)   as deduction_coupon_cost_day    --抵扣优惠券花费
    ,sum(discount_coupon_cost_day)    as discount_coupon_cost_day     --折扣优惠券花费
    ,sum(cash_coupon_cost_day)        as cash_coupon_cost_day         --现金优惠券花费
    ,sum(deduction_coupon_num_day)    as  deduction_coupon_num_day  --每天抵扣优惠券发放数量	  
    ,sum(discount_coupon_num_day)     as  discount_coupon_num_day   --每天折扣优惠券发放数量	  
    ,sum(cash_coupon_num_day)         as  cash_coupon_num_day       --每天现金优惠券发放数量	  
    ,date_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select
       nurse_home_id
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id         --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id
      ,sum(case when type = 1 then use_frequency else 0 end) as deduction_coupon_use_num_day --每天抵扣优惠券使用数量
      ,sum(case when type = 2 then use_frequency else 0 end) as discount_coupon_use_num_day  --每天折扣优惠券使用数量
      ,sum(case when type = 3 then use_frequency else 0 end) as cash_coupon_use_num_day      --每天现金优惠券使用数量
      ,sum(case when type = 1 then cost else 0 end)          as deduction_coupon_cost_day    --抵扣优惠券花费
      ,sum(case when type = 2 then cost else 0 end)          as discount_coupon_cost_day     --折扣优惠券花费
      ,sum(case when type = 3 then cost else 0 end)          as cash_coupon_cost_day         --现金优惠券花费
      ,0 as  deduction_coupon_num_day  --每天抵扣优惠券发放数量	  
      ,0 as  discount_coupon_num_day   --每天折扣优惠券发放数量	  
      ,0 as  cash_coupon_num_day       --每天现金优惠券发放数量	  
      ,substr(use_date,1,10) as date_time
  from  db_dw_hr.clife_hr_dwd_coupon_manage_info
  where part_date = regexp_replace(date_sub(current_date(),1),'-','')
  group by nurse_home_id
          ,substr(use_date,1,10)
  union all
  select
       nurse_home_id     --养老院id
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id         --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id
      ,0 as deduction_coupon_use_num_day --每天抵扣优惠券使用数量
      ,0 as discount_coupon_use_num_day  --每天折扣优惠券使用数量
      ,0 as cash_coupon_use_num_day      --每天现金优惠券使用数量
      ,0 as deduction_coupon_cost_day    --抵扣优惠券花费
      ,0 as discount_coupon_cost_day     --折扣优惠券花费
      ,0 as cash_coupon_cost_day         --现金优惠券花费
      ,sum(case when type = 1 then 1 else 0 end) as deduction_coupon_num_day  --每天抵扣优惠券发放数量
      ,sum(case when type = 2 then 1 else 0 end) as discount_coupon_num_day   --每天折扣优惠券发放数量
      ,sum(case when type = 3 then 1 else 0 end) as cash_coupon_num_day       --每天现金优惠券发放数量
      ,substr(a.coupon_issue_time,1,10) as date_time  --发放时间
  from
  (
    select
         t.nurse_home_id         --养老院id
        ,t.nursing_homes_name    --养老院名称
        ,t.sub_org_id            --机构id
        ,t.sub_org_name          --机构名称
        ,t.org_id                --集团id
        ,t.org_name              --集团名称
        ,t.area_id               --区域id
        ,t.type                  --优惠券类型
        ,ip.coupon_issue_time
    from  db_dw_hr.clife_hr_dwd_coupon_manage_info t
    lateral view explodemulti(if(t.coupon_issue_times is null,'',t.coupon_issue_times))ip as coupon_issue_time
    where t.part_date = regexp_replace(date_sub(current_date(),1),'-','')
  )a
  group by nurse_home_id
          ,substr(a.coupon_issue_time,1,10)
)t
group by nurse_home_id
        ,date_time


===========================
    clife_hr_dws_coupon_manage_by_nhome
===========================
insert overwrite table db_dw_hr.clife_hr_dws_coupon_manage_by_nhome partition(part_date)
select
     nurse_home_id     --养老院id
    ,max(nursing_homes_name) as nursing_homes_name --养老院名称
    ,max(sub_org_id)         as sub_org_id         --机构id
    ,max(sub_org_name)       as sub_org_name       --机构名称
    ,max(org_id)             as org_id             --集团id
    ,max(org_name)           as org_name           --集团名称
    ,max(area_id)            as area_id            --区域id
    ,sum(case when type = 1 then 1 else 0 end) as deduction_coupon_num                 --抵扣优惠券发放数量
    ,sum(case when type = 2 then 1 else 0 end) as discount_coupon_num                  --折扣优惠券发放数量
    ,sum(case when type = 3 then 1 else 0 end) as cash_coupon_num                      --现金优惠券发放数量
    ,sum(case when type = 1 then use_frequency else 0 end) as deduction_coupon_use_num --抵扣优惠券使用数量
    ,sum(case when type = 2 then use_frequency else 0 end) as discount_coupon_use_num  --折扣优惠券使用数量
    ,sum(case when type = 3 then use_frequency else 0 end) as cash_coupon_use_num      --现金优惠券使用数量
    ,sum(case when type = 1 then balance else 0 end) as deduction_coupon_balance       --抵扣优惠券总余额
    ,sum(case when type = 2 then balance else 0 end) as discount_coupon_balance        --折扣优惠券总余额
    ,sum(case when type = 3 then balance else 0 end) as cash_coupon_balance            --现金优惠券总余额
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from db_dw_hr.clife_hr_dwd_coupon_manage_info
where part_date = regexp_replace(date_sub(current_date(),1),'-','')
group by nurse_home_id


===========================
    clife_hr_dws_coupon_manage_by_nhmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_coupon_manage_by_nhmonth partition(part_date)
select
     nurse_home_id --养老院id
    ,max(nursing_homes_name) as nursing_homes_name --养老院名称
    ,max(sub_org_id)         as sub_org_id         --机构id
    ,max(sub_org_name)       as sub_org_name       --机构名称
    ,max(org_id)             as org_id             --集团id
    ,max(org_name)           as org_name           --集团名称
    ,max(area_id)            as area_id            --区域id 
    ,sum(deduction_coupon_num_day)    as deduction_coupon_num_month       --每月抵扣优惠券发放数量
    ,sum(discount_coupon_num_day)     as discount_coupon_num_month        --每月折扣优惠券发放数量
    ,sum(cash_coupon_num_day)         as cash_coupon_num_month            --每月现金优惠券发放数量
    ,sum(deduction_coupon_use_num_day)as deduction_coupon_use_num_month   --每月抵扣优惠券使用数量
    ,sum(discount_coupon_use_num_day) as discount_coupon_use_num_month    --每月折扣优惠券使用数量
    ,sum(cash_coupon_use_num_day)     as cash_coupon_use_num_month        --每月现金优惠券使用数量
    ,sum(deduction_coupon_cost_day)   as deduction_coupon_cost_month      --每月抵扣优惠券花费
    ,sum(discount_coupon_cost_day)    as discount_coupon_cost_month       --每月折扣优惠券花费
    ,sum(cash_coupon_cost_day)        as cash_coupon_cost_month           --每月现金优惠券花费
    ,substr(date_time,1,7) as date_month  --月期 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from db_dw_hr.clife_hr_dws_coupon_manage_by_nhday
where part_date = regexp_replace(date_sub(current_date(),1),'-','')
group by nurse_home_id
        ,substr(date_time,1,7)


===========================
    clife_hr_dws_activity_manage_by_nhday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_nhday partition(part_date)
select
     nurse_home_id
    ,max(nursing_homes_name) as nursing_homes_name     --养老院名称
    ,max(sub_org_id)         as sub_org_id             --机构id
    ,max(sub_org_name)       as sub_org_name           --机构名称
    ,max(org_id)             as org_id                --集团id
    ,max(org_name)           as org_name              --集团名称
    ,max(area_id)            as area_id               --区域id
    ,sum(apply_old_man_cnt_day)  as apply_old_man_cnt_day  --报名人次
    ,sum(apply_activity_cnt_day) as apply_activity_cnt_day --已报名活动数
    ,sum(sign_old_man_cnt_day)   as sign_old_man_cnt_day   --签到人次
    ,sum(confirm_old_man_cnt_day)as confirm_old_man_cnt_day--到场人次
    ,sum(host_activity_cnt_day)  as host_activity_cnt_day  --已举办活动数
    ,sum(template_cnt_day)        as template_cnt_day        --使用活动模板数量
    ,sum(activity_cnt_day)       as activity_cnt_day       --已创建活动数
    ,sum(activity_fee_day)       as activity_fee_day       --活动费用
    ,sum(reserve_cnt_day)        as reserve_cnt_day        --预定活动室次数
    ,date_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select
       nurse_home_id
      ,max(nursing_homes_name) as nursing_homes_name     --养老院名称
      ,max(sub_org_id)         as sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name           --机构名称
      ,max(org_id)             as org_id                --集团id
      ,max(org_name)           as org_name              --集团名称
      ,max(area_id)            as area_id               --区域id
      ,count(t.apply_time)           as apply_old_man_cnt_day        --报名人次
      ,count(t.apply_time)           as apply_activity_cnt_day       --已报名活动数
      ,0 as sign_old_man_cnt_day   --签到人次
      ,0 as confirm_old_man_cnt_day--到场人次
      ,0 as host_activity_cnt_day  --已举办活动数
  	  ,0 as template_cnt_day        --使用活动模板数量
  	  ,0 as activity_cnt_day       --已创建活动数
  	  ,0 as activity_fee_day       --活动费用
      ,0 as reserve_cnt_day  --预定活动室次数
      ,substr(t.apply_time,1,10)     as date_time
  from
  (
    select
         a.activity_id
        ,a.nurse_home_id
        ,a.nursing_homes_name     --养老院名称
        ,a.sub_org_id             --机构id
        ,a.sub_org_name           --机构名称
        ,a.org_id                --集团id
        ,a.org_name              --集团名称
        ,a.area_id               --区域id
        ,ip.apply_time
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    lateral view explodemulti(if(a.apply_times is null,'',a.apply_times)) ip as apply_time
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )t
  group by nurse_home_id
          ,substr(t.apply_time,1,10)
  union all
  select
       nurse_home_id
      ,max(nursing_homes_name) as nursing_homes_name     --养老院名称
      ,max(sub_org_id)         as sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name           --机构名称
      ,max(org_id)             as org_id                --集团id
      ,max(org_name)           as org_name              --集团名称
      ,max(area_id)            as area_id               --区域id
      ,0 as apply_old_man_cnt_day        --报名人次
      ,0 as apply_activity_cnt_day       --已报名活动数
      ,count(t.sign_old_man)       as sign_old_man_cnt_day       --签到人次
      ,0 as confirm_old_man_cnt_day--到场人次
      ,0 as host_activity_cnt_day  --已举办活动数
      ,0 as template_cnt_day        --使用活动模板数量
      ,0 as activity_cnt_day       --已创建活动数
      ,0 as activity_fee_day       --活动费用
      ,0 as reserve_cnt_day  --预定活动室次数
      ,substr(t.sign_time,1,10) as date_time
  from
  (
    select
         a.activity_id
        ,a.nurse_home_id
        ,a.nursing_homes_name     --养老院名称
        ,a.sub_org_id             --机构id
        ,a.sub_org_name           --机构名称
        ,a.org_id                --集团id
        ,a.org_name              --集团名称
        ,a.area_id               --区域id
        ,ip.sign_old_man
        ,ip.sign_time
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    lateral view explodemulti(if(a.sign_old_mans is null,'',a.sign_old_mans),if(a.sign_times is null,'',a.sign_times))ip as sign_old_man,sign_time
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )t
  group by nurse_home_id
          ,substr(t.sign_time,1,10)
  union all
  select
       nurse_home_id
      ,max(nursing_homes_name) as nursing_homes_name     --养老院名称
      ,max(sub_org_id)         as sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name           --机构名称
      ,max(org_id)             as org_id                --集团id
      ,max(org_name)           as org_name              --集团名称
      ,max(area_id)            as area_id               --区域id
      ,0 as apply_old_man_cnt_day        --报名人次
      ,0 as apply_activity_cnt_day       --已报名活动数
      ,0 as sign_old_man_cnt_day       --签到人次
      ,count(t.confirm_old_man)  as confirm_old_man_cnt_day       --到场人次
      ,0 as host_activity_cnt_day  --已举办活动数
  	,0 as template_cnt_day   --使用活动模板数量
  	,0 as activity_cnt_day  --已创建活动数
  	,0 as activity_fee_day   --活动费用
      ,0 as reserve_cnt_day  --预定活动室次数
      ,substr(t.confirm_time,1,10) as date_time
  from
  (
    select
         a.activity_id
        ,a.nurse_home_id
        ,a.nursing_homes_name     --养老院名称
        ,a.sub_org_id             --机构id
        ,a.sub_org_name           --机构名称
        ,a.org_id                --集团id
        ,a.org_name              --集团名称
        ,a.area_id               --区域id
        ,ip.confirm_old_man
        ,ip.confirm_time
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    lateral view explodemulti(if(a.confirm_old_mans is null,'',a.confirm_old_mans),if(a.confirm_times is null,'',a.confirm_times))ip as confirm_old_man,confirm_time
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )t
  group by nurse_home_id
          ,substr(t.confirm_time,1,10)
  union all	 
  select
       nurse_home_id 
      ,max(nursing_homes_name) as nursing_homes_name     --养老院名称
      ,max(sub_org_id)         as sub_org_id             --机构id
      ,max(sub_org_name)       as sub_org_name           --机构名称
      ,max(org_id)             as org_id                --集团id
      ,max(org_name)           as org_name              --集团名称
      ,max(area_id)            as area_id               --区域id
      ,0 as apply_old_man_cnt_day        --报名人次
      ,0 as apply_activity_cnt_day       --已报名活动数
      ,0 as sign_old_man_cnt_day         --签到人次
      ,0 as confirm_old_man_cnt_day      --到场人次
      ,count(activity_id) as host_activity_cnt_day  --已举办活动数
  	,0 as template_cnt_day   --使用活动模板数量
  	,0 as activity_cnt_day  --已创建活动数
  	,0 as activity_fee_day   --活动费用
      ,0 as reserve_cnt_day  --预定活动室次数
      ,substr(activity_time,1,10) as date_time --活动创建时间
  from db_dw_hr.clife_hr_dwd_activity_manage_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nurse_home_id
          ,substr(activity_time,1,10) 
  union all
  select
       nurse_home_id
      ,max(nursing_homes_name)as nursing_homes_name     --养老院名称
      ,max(sub_org_id)        as sub_org_id             --机构id
      ,max(sub_org_name)      as sub_org_name           --机构名称
      ,max(org_id)             as org_id                --集团id
      ,max(org_name)           as org_name              --集团名称
      ,max(area_id)            as area_id               --区域id
      ,0 as apply_old_man_cnt_day        --报名人次
      ,0 as apply_activity_cnt_day       --已报名活动数
      ,0 as sign_old_man_cnt_day         --签到人次
      ,0 as confirm_old_man_cnt_day      --到场人次
      ,0 as host_activity_cnt_day  --已举办活动数
      ,count(template_id) as template_cnt_day   --使用活动模板数量
      ,count(activity_id) as activity_cnt_day  --已创建活动数
      ,sum(activity_fee)  as activity_fee_day   --活动费用
      ,0 as reserve_cnt_day  --预定活动室次数
      ,substr(activity_create_time,1,10) as date_time --活动创建时间
  from db_dw_hr.clife_hr_dwd_activity_manage_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nurse_home_id
          ,substr(activity_create_time,1,10)
  union all
  select
       nurse_home_id
      ,max(nursing_homes_name)as nursing_homes_name     --养老院名称
      ,max(sub_org_id)        as sub_org_id             --机构id
      ,max(sub_org_name)      as sub_org_name           --机构名称
      ,max(org_id)             as org_id                  --集团id
      ,max(org_name)           as org_name                --集团名称
      ,max(area_id)            as area_id                 --区域id
      ,0 as apply_old_man_cnt_day        --报名人次
      ,0 as apply_activity_cnt_day       --已报名活动数
      ,0 as sign_old_man_cnt_day         --签到人次
      ,0 as confirm_old_man_cnt_day      --到场人次
      ,0 as host_activity_cnt_day  --已举办活动数
      ,0 as template_cnt_day   --使用活动模板数量
      ,0 as activity_cnt_day  --已创建活动数
      ,0 as activity_fee_day   --活动费用
      ,count(t.reserve_id) as reserve_cnt_day  --预定活动室次数
      ,substr(t.reserve_time,1,10) as date_time
  from
  (
    select
         a.activity_id
        ,a.nurse_home_id
        ,a.nursing_homes_name   --养老院名称
        ,a.sub_org_id           --机构id
        ,a.sub_org_name         --机构名称
        ,a.org_id               --集团id
        ,a.org_name             --集团名称
        ,a.area_id              --区域id
        ,ip.reserve_id
        ,ip.reserve_time
    from db_dw_hr.clife_hr_dwd_activity_manage_info a
    lateral view explodemulti(if(a.reserve_ids is null,'',a.reserve_ids),if(a.reserve_times is null,'',a.reserve_times))ip as reserve_id,reserve_time
    where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')
  )t
  group by nurse_home_id
          ,substr(t.reserve_time,1,10)
)k
group by nurse_home_id
        ,date_time


===========================
    clife_hr_dws_activity_manage_by_nhmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_nhmonth partition(part_date)
select
     nurse_home_id --养老院id
    ,nursing_homes_name     --养老院名称
    ,sub_org_id             --机构id
    ,sub_org_name           --机构名称
    ,org_id                 --集团id
    ,org_name               --集团名称
    ,area_id                --区域id 
    ,reserve_cnt_month        --预定活动室次数
    ,template_cnt_month        --使用模板次数
    ,activity_cnt_month       --创建活动数
    ,activity_fee_month       --活动费用
    ,host_activity_cnt_month  --已举办活动数
    ,confirm_old_man_cnt_month--到场人次
    ,sign_old_man_cnt_month   --签到人次
    ,apply_old_man_cnt_month  --报名人次
    ,apply_activity_cnt_month --已报名活动数
    ,date_month --月份 
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from
(
  select
       nurse_home_id --养老院id
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id         --机构id
      ,max(sub_org_name)       as sub_org_name       --机构名称
      ,max(org_id)             as org_id             --集团id
      ,max(org_name)           as org_name           --集团名称
      ,max(area_id)            as area_id            --区域id 
      ,sum(reserve_cnt_day)         as reserve_cnt_month        --预定活动室次数
      ,sum(template_cnt_day)         as template_cnt_month        --使用模板次数
      ,sum(activity_cnt_day)        as activity_cnt_month       --创建活动数
      ,sum(activity_fee_day)        as activity_fee_month       --活动费用
      ,sum(host_activity_cnt_day)   as host_activity_cnt_month  --已举办活动数
      ,sum(confirm_old_man_cnt_day) as confirm_old_man_cnt_month--到场人次
      ,sum(sign_old_man_cnt_day)    as sign_old_man_cnt_month   --签到人次
      ,sum(apply_old_man_cnt_day)   as apply_old_man_cnt_month  --报名人次
      ,sum(apply_activity_cnt_day)  as apply_activity_cnt_month --已报名活动数
      ,substr(date_time,1,7) as date_month --月份     
  from db_dw_hr.clife_hr_dws_activity_manage_by_nhday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nurse_home_id
          ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_activity_manage_by_suborg
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_suborg partition(part_date)
select
     activity_type           --活动类型,
	,sub_org_id             --机构id
    ,max(sub_org_name)      as sub_org_name           --机构名称
    ,max(org_id)             as org_id                  --集团id
    ,max(org_name)           as org_name                --集团名称
    ,max(area_id)            as area_id                 --区域id
    ,sum(reserve_num)       as reserve_num            --预定活动室次数
    ,sum(template_cnt)       as template_cnt            --使用活动模板数量
    ,sum(activity_num)      as activity_num           --已创建活动数
    ,sum(apply_activity_cnt)as apply_activity_cnt     --已报名活动数
    ,sum(host_activity_cnt) as host_activity_cnt      --已举办活动数
    ,sum(apply_old_man_cnt) as apply_old_man_cnt      --报名人次
    ,sum(sign_old_man_cnt)  as sign_old_man_cnt       --签到人次
    ,sum(confirm_old_man_cnt)  as confirm_old_man_cnt       --到场人次
    ,sum(activity_fee)      as activity_fee           --活动费用
    ,sum(good_evaluation_num)   as good_evaluation_num    --活动评价好评数
    ,sum(middle_evaluation_num) as middle_evaluation_num  --活动评价中评数
    ,sum(bad_evaluation_num)    as bad_evaluation_num     --活动评价差评数
    ,sum(good_feedback_num)     as good_feedback_num      --反馈非常好数
    ,sum(middle_feedback_num)   as middle_feedback_num    --反馈较好数
    ,sum(general_feedback_num)  as general_feedback_num   --反馈一般数
    ,sum(bad_feedback_num)      as bad_feedback_num       --反馈不理想数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from db_dw_hr.clife_hr_dwm_activity_manage_aggregation
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by activity_type
        ,sub_org_id


===========================
    clife_hr_dws_activity_manage_by_nhome
===========================
insert overwrite table db_dw_hr.clife_hr_dws_activity_manage_by_nhome partition(part_date)
select
     nurse_home_id           --养老院id
    ,max(nursing_homes_name)as nursing_homes_name     --养老院名称
    ,max(sub_org_id)        as sub_org_id             --机构id
    ,max(sub_org_name)      as sub_org_name           --机构名称
    ,max(org_id)             as org_id                  --集团id
    ,max(org_name)           as org_name                --集团名称
    ,max(area_id)            as area_id                 --区域id
    ,sum(reserve_num)       as reserve_num           --预定活动室次数
    ,sum(template_cnt)       as emplate_cnt            --使用活动模板数量
    ,sum(activity_num)      as activity_num           --已创建活动数
    ,sum(apply_activity_cnt)as apply_activity_cnt     --已报名活动数
    ,sum(host_activity_cnt) as host_activity_cnt      --已举办活动数
    ,sum(apply_old_man_cnt) as apply_old_man_cnt      --报名人次
    ,sum(sign_old_man_cnt)  as sign_old_man_cnt       --签到人次
    ,sum(confirm_old_man_cnt)  as confirm_old_man_cnt       --到场人次
    ,sum(activity_fee)      as activity_fee           --活动费用
    ,sum(good_evaluation_num)   as good_evaluation_num    --活动评价好评数
    ,sum(middle_evaluation_num) as middle_evaluation_num  --活动评价中评数
    ,sum(bad_evaluation_num)    as bad_evaluation_num     --活动评价差评数
    ,sum(good_feedback_num)     as good_feedback_num      --反馈非常好数
    ,sum(middle_feedback_num)   as middle_feedback_num    --反馈较好数
    ,sum(general_feedback_num)  as general_feedback_num   --反馈一般数
    ,sum(bad_feedback_num)      as bad_feedback_num       --反馈不理想数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --日期分区
from db_dw_hr.clife_hr_dwm_activity_manage_aggregation
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
group by nurse_home_id


===========================
    clife_hr_dws_service_record_by_nhday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_nhday partition(part_date)
select
     nursin_homes_id  --养老院id
    ,service_type     --服务类型（增值or基础）
    ,nursing_homes_name    --养老院名称
    ,sub_org_id    --机构id
    ,sub_org_name  --机构名称
    ,org_id        --集团id
    ,org_name      --集团名称
    ,old_man_num_day    --养老院老人数
    ,cost_day                          --服务总费用
    ,service_num_day                   --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_day/old_man_num_day,0) as recharge_avg_day --人均服务费
    ,coalesce(service_num_day/old_man_num_day,0) as member_recharge_avg_day  --人均服务数
    ,date_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from 
(
  select
       nursin_homes_id
      ,service_type
      ,max(nursing_homes_name) as nursing_homes_name --养老院名称
      ,max(sub_org_id)         as sub_org_id--机构id
      ,max(sub_org_name)       as sub_org_name--机构名称
      ,max(org_id)             as org_id--集团id
      ,max(org_name)           as org_name--集团名称
      ,substr(create_time,1,10) as date_time
      ,count(distinct old_man_id) as old_man_num_day --养老院老人数
      ,sum(cost) as cost_day        --服务总费用
      ,sum(1)    as service_num_day --服务数
      ,sum(case when settlement_status =  2 then 1 else 0 end ) as  already_settle_service_num     --已结算服务数
      ,sum(case when settlement_status =  3 then 1 else 0 end ) as  no_settle_service_num          --未结算服务数
      ,sum(case when service_status = 1  then 1 else 0 end )    as  wait_complete_service_num      --待完成服务数
      ,sum(case when service_status = 2  then 1 else 0 end )    as  complete_service_num           --已完成服务数
      ,sum(case when service_status = 3  then 1 else 0 end )    as  evaluation_service_num         --已评价服务数
      ,sum(case when evaluation_status = 0  then 1 else 0 end ) as  evaluation_wait_num            --待评价服务数
      ,sum(case when evaluation_status = 1  then 1 else 0 end ) as  evaluation_good_num            --好评数
      ,sum(case when evaluation_status = 2  then 1 else 0 end ) as  evaluation_middle_num          --中评数
      ,sum(case when evaluation_status = 3  then 1 else 0 end ) as  evaluation_bad_num             --差评数
  from db_dw_hr.clife_hr_dwd_service_record_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by nursin_homes_id
          ,service_type
          ,substr(create_time,1,10)
)t



===========================
    clife_hr_dws_service_record_by_spstyear
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_spstyear partition(part_date)
select
     service_project_id --服务项目id
    ,sub_org_id         --机构id
    ,service_type       --服务类型（增值or基础）
    ,service_project_name  --服务项目名称
    ,service_type_id       --服务类型id
    ,service_type_name     --服务类型id名
    ,sub_org_name          --机构名称
    ,org_id                --集团id
    ,org_name              --集团名称
    ,old_man_num_year    --养老院老人数
    ,cost_year                    --服务总费用
    ,service_num_year             --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_year/old_man_num_year,0)        as recharge_avg_year         --人均服务费
    ,coalesce(service_num_year/old_man_num_year,0) as member_recharge_avg_year  --人均服务数
    ,date_year
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
   select
        service_project_id --服务项目id
       ,sub_org_id         --机构id
       ,service_type       --服务类型（增值or基础）
       ,max(service_project_name)   as service_project_name  --服务项目名称
       ,max(service_type_id)        as service_type_id       --服务类型id
       ,max(service_type_name)      as service_type_name     --服务类型id名
       ,max(sub_org_name) as sub_org_name  --机构名称
       ,max(org_id)       as org_id        --集团id
       ,max(org_name)     as org_name      --集团名称
       ,sum(old_man_num_month)            as old_man_num_year              --养老院老人数
       ,sum(cost_month)                   as cost_year                    --服务总费用
       ,sum(service_num_month)            as service_num_year             --服务数
       ,sum(already_settle_service_num) as already_settle_service_num     --已结算服务数
       ,sum(no_settle_service_num)      as no_settle_service_num          --未结算服务数
       ,sum(wait_complete_service_num)  as wait_complete_service_num      --待完成服务数
       ,sum(complete_service_num)       as complete_service_num           --已完成服务数
       ,sum(evaluation_service_num)     as evaluation_service_num         --已评价服务数
       ,sum(evaluation_wait_num)        as evaluation_wait_num            --待评价服务数
       ,sum(evaluation_good_num)        as evaluation_good_num            --好评数
       ,sum(evaluation_middle_num)      as evaluation_middle_num          --中评数
       ,sum(evaluation_bad_num)         as evaluation_bad_num             --差评数
       ,substr(date_month,1,4) as date_year
   from db_dw_hr.clife_hr_dws_service_record_by_spstmonth
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by service_project_id
          ,sub_org_id
          ,service_type
          ,substr(date_month,1,4)
)t


===========================
    clife_hr_dws_service_record_by_spstday
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_spstday partition(part_date)
select
     service_project_id    --服务项目id
    ,sub_org_id            --机构id
    ,service_type          --服务类型（增值or基础）
    ,service_project_name  --服务项目名称
    ,service_type_id       --服务类型id
    ,service_type_name     --服务类型id名
    ,sub_org_name          --机构名称
    ,org_id                --集团id
    ,org_name              --集团名称
    ,old_man_num_day       --养老院老人数
    ,cost_day                          --服务总费用
    ,service_num_day                   --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_day/old_man_num_day,0)        as recharge_avg_day --人均服务费
    ,coalesce(service_num_day/old_man_num_day,0) as member_recharge_avg_day  --人均服务数
    ,date_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from 
(
 select
       service_project_id --服务项目id
      ,sub_org_id         --机构id
      ,service_type       --服务类型（增值or基础）
      ,max(service_project_name)   as service_project_name  --服务项目名称
      ,max(service_type_id)        as service_type_id       --服务类型id
      ,max(service_type_name)      as service_type_name     --服务类型id名
      ,max(sub_org_name) as sub_org_name  --机构名称
      ,max(org_id)       as org_id        --集团id
      ,max(org_name)     as org_name      --集团名称
      ,sum(cost)         as cost_day                          --服务总费用
      ,sum(1)            as service_num_day                   --服务数
      ,count(distinct old_man_id) as old_man_num_day --养老院老人数
      ,sum(case when settlement_status =  2 then 1 else 0 end ) as  already_settle_service_num     --已结算服务数
      ,sum(case when settlement_status =  3 then 1 else 0 end ) as  no_settle_service_num          --未结算服务数
      ,sum(case when service_status = 1  then 1 else 0 end )    as  wait_complete_service_num      --待完成服务数
      ,sum(case when service_status = 2  then 1 else 0 end )    as  complete_service_num           --已完成服务数
      ,sum(case when service_status = 3  then 1 else 0 end )    as  evaluation_service_num         --已评价服务数
      ,sum(case when evaluation_status = 0  then 1 else 0 end ) as  evaluation_wait_num            --待评价服务数
      ,sum(case when evaluation_status = 1  then 1 else 0 end ) as  evaluation_good_num            --好评数
      ,sum(case when evaluation_status = 2  then 1 else 0 end ) as  evaluation_middle_num          --中评数
      ,sum(case when evaluation_status = 3  then 1 else 0 end ) as  evaluation_bad_num             --差评数
      ,substr(create_time,1,10) as date_time
  from db_dw_hr.clife_hr_dwd_service_record_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by service_project_id
          ,sub_org_id
          ,service_type
          ,substr(create_time,1,10)
)t



===========================
    clife_hr_dws_service_record_by_nhmonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_nhmonth partition(part_date)
select
     nursin_homes_id  --养老院id
    ,service_type     --服务类型（增值or基础）
    ,nursing_homes_name    --养老院名称
    ,sub_org_id    --机构id
    ,sub_org_name  --机构名称
    ,org_id        --集团id
    ,org_name      --集团名称
    ,old_man_num_month    --养老院老人数
    ,cost_month                    --服务总费用
    ,service_num_month             --服务数
    ,already_settle_service_num    --已结算服务数
    ,no_settle_service_num         --未结算服务数
    ,wait_complete_service_num     --待完成服务数
    ,complete_service_num          --已完成服务数
    ,evaluation_service_num        --已评价服务数
    ,evaluation_wait_num           --待评价服务数
    ,evaluation_good_num           --好评数
    ,evaluation_middle_num         --中评数
    ,evaluation_bad_num            --差评数
    ,coalesce(cost_month/old_man_num_month,0) as recharge_avg_month                --人均服务费
    ,coalesce(service_num_month/old_man_num_month,0) as member_recharge_avg_month  --人均服务数
    ,date_month
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
   select
        nursin_homes_id        --养老院id
       ,service_type           --服务类型（增值or基础）
       ,max(nursing_homes_name) as nursing_homes_name --养老院名称
       ,max(sub_org_id)         as sub_org_id--机构id
       ,max(sub_org_name)       as sub_org_name--机构名称
       ,max(org_id)             as org_id--集团id
       ,max(org_name)           as org_name--集团名称
       ,sum(old_man_num_day)            as old_man_num_month              --养老院老人数
       ,sum(cost_day)                   as cost_month                     --服务总费用
       ,sum(service_num_day)            as service_num_month              --服务数
       ,sum(already_settle_service_num) as already_settle_service_num     --已结算服务数
       ,sum(no_settle_service_num)      as no_settle_service_num          --未结算服务数
       ,sum(wait_complete_service_num)  as wait_complete_service_num      --待完成服务数
       ,sum(complete_service_num)       as complete_service_num           --已完成服务数
       ,sum(evaluation_service_num)     as evaluation_service_num         --已评价服务数
       ,sum(evaluation_wait_num)        as evaluation_wait_num            --待评价服务数
       ,sum(evaluation_good_num)        as evaluation_good_num            --好评数
       ,sum(evaluation_middle_num)      as evaluation_middle_num          --中评数
       ,sum(evaluation_bad_num)         as evaluation_bad_num             --差评数
       ,substr(date_time,1,7) as date_month
   from db_dw_hr.clife_hr_dws_service_record_by_nhday
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by nursin_homes_id
           ,service_type
           ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_service_record_by_nhome
===========================
insert overwrite table db_dw_hr.clife_hr_dws_service_record_by_nhome partition(part_date)
select
     a.nursin_homes_id  --养老院id
    ,a.service_type     --服务类型（增值or基础）
    ,a.nursing_homes_name    --养老院名称
    ,a.sub_org_id    --机构id
    ,a.sub_org_name  --机构名称
    ,a.org_id        --集团id
    ,a.org_name      --集团名称
    ,b.old_man_num    --养老院老人数
    ,a.cost                          --服务总费用
    ,a.service_cnt                   --服务数
    ,a.already_settle_service_num    --已结算服务数
    ,a.no_settle_service_num         --未结算服务数
    ,a.wait_complete_service_num     --待完成服务数
    ,a.complete_service_num          --已完成服务数
    ,a.evaluation_service_num        --已评价服务数
    ,a.evaluation_wait_num           --待评价服务数
    ,a.evaluation_good_num           --好评数
    ,a.evaluation_middle_num         --中评数
    ,a.evaluation_bad_num            --差评数
    ,coalesce(a.cost/b.old_man_num,0) as recharge_avg_day --人均服务费
    ,coalesce(a.service_cnt/b.old_man_num,0) as member_recharge_avg_day  --人均服务数
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date  --分区日期
from
(
  select
       nursin_homes_id  --养老院id
      ,service_type     --服务类型（增值or基础）
      ,max(nursing_homes_name)   as nursing_homes_name    --养老院名称
      ,max(sub_org_id)   as sub_org_id    --机构id
      ,max(sub_org_name) as sub_org_name  --机构名称
      ,max(org_id)       as org_id        --集团id
      ,max(org_name)     as org_name      --集团名称
      ,sum(cost)                       as cost                          --服务总费用
      ,sum(service_cnt)                as service_cnt                   --服务数
      ,sum(already_settle_service_num) as already_settle_service_num    --已结算服务数
      ,sum(no_settle_service_num)      as no_settle_service_num         --未结算服务数
      ,sum(wait_complete_service_num)  as wait_complete_service_num     --待完成服务数
      ,sum(complete_service_num)       as complete_service_num          --已完成服务数
      ,sum(evaluation_service_num)     as evaluation_service_num        --已评价服务数
      ,sum(evaluation_wait_num)        as evaluation_wait_num           --待评价服务数
      ,sum(evaluation_good_num)        as evaluation_good_num           --好评数
      ,sum(evaluation_middle_num)      as evaluation_middle_num         --中评数
      ,sum(evaluation_bad_num)         as evaluation_bad_num            --差评数
  from db_dw_hr.clife_hr_dwm_service_record_aggregation
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by  nursin_homes_id
           ,service_type
)a
left join 
(
  select
       nursin_homes_id  --养老院id
      ,service_type     --服务类型（增值or基础）
      ,count(distinct old_man_id) as old_man_num --养老院老人数
  from db_dw_hr.clife_hr_dwd_service_record_info
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by  nursin_homes_id
           ,service_type
)b on a.nursin_homes_id = b.nursin_homes_id and a.service_type = b.service_type


===========================
    clife_hr_dws_plan_snapshot_by_protypemonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_protypemonth partition(part_date)
select
     sub_org_id         --机构id
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_month  --计划总数
    ,item_num_month  --任务总数
    ,complete_num_month --完成任务数
    ,coalesce(complete_num_month/item_num_month,0) as complete_rate_month --完成率
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       sub_org_id        --机构id
      ,project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_day) as plan_num_month  --计划总数
      ,sum(item_num_day) as item_num_month  --任务总数
      ,sum(complete_num_day) as complete_num_month --完成任务数
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_plan_snapshot_by_protypeday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by project_type_id
           ,sub_org_id
           ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_plan_snapshot_by_protype
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_protype partition(part_date)
select
     sub_org_id         --机构id
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num  --计划总数
    ,item_num  --任务总数
    ,complete_num --完成任务数
    ,coalesce(complete_num/item_num,0) as complete_rate --完成率
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
   select
        sub_org_id        --机构id
       ,project_type_id   --项目类型id
       ,max(project_type_name)as project_type_name  --项目类型名称
       ,max(sub_org_name)     as sub_org_name       --机构名称
       ,max(org_id)           as org_id             --集团id
       ,max(org_name)         as org_name           --集团名称
       ,count(exec_date) as plan_num  --计划总数
       ,count(1) as  item_num  --任务总数
       ,sum(case when complete_stase = 1 then 1 else 0 end) as complete_num --完成任务数
   from db_dw_hr.clife_hr_dwd_plan_snapshot_info
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by project_type_id
           ,sub_org_id  
)t


===========================
    clife_hr_dws_plan_snapshot_by_promonth
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_promonth partition(part_date)
select
     project_id         --项目id
    ,sub_org_id         --机构id
    ,project_name       --项目名称
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num_month  --计划总数
    ,item_num_month  --任务总数
    ,complete_num_month --完成任务数
    ,coalesce(complete_num_month/item_num_month,0) as complete_rate_month --完成率
    ,date_month --月份
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
  select
       project_id        --项目id
      ,sub_org_id        --机构id
      ,max(project_name)     as project_name       --项目名称
      ,max(project_type_id)  as project_type_id    --项目类型id
      ,max(project_type_name)as project_type_name  --项目类型名称
      ,max(sub_org_name)     as sub_org_name       --机构名称
      ,max(org_id)           as org_id             --集团id
      ,max(org_name)         as org_name           --集团名称
      ,sum(plan_num_day) as plan_num_month  --计划总数
      ,sum(item_num_day) as item_num_month  --任务总数
      ,sum(complete_num_day) as complete_num_month --完成任务数
      ,substr(date_time,1,7) as date_month --月份
  from db_dw_hr.clife_hr_dws_plan_snapshot_by_proday
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by project_id
           ,sub_org_id
           ,substr(date_time,1,7)
)t


===========================
    clife_hr_dws_plan_snapshot_by_project
===========================
insert overwrite table db_dw_hr.clife_hr_dws_plan_snapshot_by_project partition(part_date)
select
     project_id         --项目id
    ,sub_org_id         --机构id
    ,project_name       --项目名称
    ,project_type_id    --项目类型id
    ,project_type_name  --项目类型名称
    ,sub_org_name       --机构名称
    ,org_id             --集团id
    ,org_name           --集团名称
    ,plan_num  --计划总数
    ,item_num  --任务总数
    ,complete_num --完成任务数
    ,coalesce(complete_num/item_num,0) as complete_rate --完成率
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date --分区日期
from
(
   select
        project_id        --项目id
       ,sub_org_id        --机构id
       ,max(project_name)     as project_name       --项目名称
       ,max(project_type_id)  as project_type_id    --项目类型id
       ,max(project_type_name)as project_type_name  --项目类型名称
       ,max(sub_org_name)     as sub_org_name       --机构名称
       ,max(org_id)           as org_id             --集团id
       ,max(org_name)         as org_name           --集团名称
       ,count(exec_date) as plan_num  --计划总数
       ,count(1) as  item_num  --任务总数
       ,sum(case when complete_stase = 1 then 1 else 0 end) as complete_num --完成任务数
   from db_dw_hr.clife_hr_dwd_plan_snapshot_info
   where part_date=regexp_replace(date_sub(current_date(),1),'-','')
   group by project_id
           ,sub_org_id  
)t


