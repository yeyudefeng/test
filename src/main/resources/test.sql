clife_hr_dws_crm_cust_building_info_tmp

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

==========================

clife_hr_dws_crm_member_building_info_tmp



create table db_dw_hr.clife_hr_dws_crm_member_building_info_tmp stored as parquet as
select
 t.membership_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.sub_org_name)   sub_org_name                    --  '养老机构名称'
,max(t.org_id)      org_id                      --  '集团id'
,max(t.org_name)    org_name                       --  '集团名称'
,max(t.area_id)     area_id                       --  '区域id'
,max(t.area_name)   area_name                       --  '区域名称'
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




==========================

insert overwrite table db_dw_hr.clife_hr_dws_market_by_building_f partition(part_date)
select
bb.building_id as building_id      --'楼栋'
,bb.sub_org_id as sub_org_id                          --   '养老机构id',
,bb.sub_org_name as sub_org_name                        --   '养老机构名称'
,bb.org_id as org_id                              --   '养老集团id'
,bb.org_id as org_name                            --   '养老集团名称'
,bb.area_id as area_id                             --   '区域id'
,bb.area_name as area_name                           --   '区域名称'
,bb.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'
,bb.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'
,bb.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'
,bb.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'
,bb.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'
,bb.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'
,bb.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'
,bb.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'
,bb.crm_intent_cnt                      --   '营销管理模块意向客户数'
,bb.deposit_cust_cnt                    --   '下定客户数'
,bb.sign_cust_cnt                       --   '签约客户数'
,bb.transfer_to_member_cnt              --   '转让会籍数'
,bb.transfer_from_member_cnt            --   '继承会籍数'
,bb.withdraw_member_cnt                 --   '退会会籍数'
,bb.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'
,bb.deposit_cust_visit_cnt              --   '下定客户回访次数'
,bb.sign_cust_visit_cnt as sign_cust_visit_cnt                 --   '签约客户回访次数'
,bb.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,bb.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,bb.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,bb.apply_member_cnt                    --   '预申请会籍数量'
,bb.deposit_member_cnt                  --   '下定会籍数量'
,d1.deposit_room_cnt                    --   '下定房间数量'
,bb.sign_member_cnt                     --   '签约会籍数量'
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
,bb.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,bb.assess_cnt  as assess_cnt                          --   '评估总次数'
,regexp_replace(date_sub(current_date(),1),'-','')  as part_date
from
(
select
 if(b1.building_id   is null, c1.building_id  ,b1.building_id  )     as building_id      --'楼栋'
,if(b1.sub_org_id    is null, c1.sub_org_id   ,b1.sub_org_id   )     as sub_org_id                          --   '养老机构id',
,if(b1.sub_org_name  is null, c1.sub_org_name ,b1.sub_org_name )     as sub_org_name                        --   '养老机构名称'
,if(b1.org_id        is null, c1.org_id       ,b1.org_id       )     as org_id                              --   '养老集团id'
,if(b1.org_id        is null, c1.org_id       ,b1.org_id       )     as org_name                            --   '养老集团名称'
,if(b1.area_id       is null, c1.area_id      ,b1.area_id      )     as area_id                             --   '区域id'
,if(b1.area_name     is null, c1.area_name    ,b1.area_name    )     as area_name                           --   '区域名称'
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
,b1.sign_cust_visit_cnt as sign_cust_visit_cnt                 --   '签约客户回访次数'
,b1.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'
,b1.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'
,b1.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'
,c1.apply_member_cnt                    --   '预申请会籍数量'
,c1.deposit_member_cnt                  --   '下定会籍数量'
,c1.sign_member_cnt                     --   '签约会籍数量'
,b1.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'
,b1.assess_cnt  as assess_cnt                          --   '评估总次数'
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
,sum(sign_cust_visit_cnt)   as   sign_cust_visit_cnt    -- '签约客户回访总次数'
,sum(deposit_cust_visit_cnt)/sum(deposit_cust_cnt)  as avg_deposit_cust_visit_cnt  --'下定客户平均回访次数'
,sum(sign_cust_visit_cnt)/sum(sign_cust_cnt)  as avg_sign_cust_visit_cnt  --'签约客户平均回访次数'
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
,case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_visit_cnt
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
,max(t1.sub_org_name) sub_org_name
,max(t1.org_id      ) org_id
,max(t1.org_name    ) org_name
,max(t1.area_id     ) area_id
,max(t1.area_name   ) area_name
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
,c.sub_org_name
,c.org_id
,c.org_name
,c.area_id
,c.area_name
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
,t.sub_org_name
,t.org_id
,t.org_name
,t.area_id
,t.area_name
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
) bb
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
on bb.sub_org_id=d1.sub_org_id  and bb.building_id=d1.building_id
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
on bb.sub_org_id=e1.sub_org_id  and bb.building_id=e1.building_id


==========================
clife_hr_customer_crm_visit_building_info_tmp

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


==========================

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


==========================


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

==========================


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
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_month=tt10.withdraw_month

==========================

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


==========================

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


==========================

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


==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================




==========================


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










