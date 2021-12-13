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
,cast(l1.building_id as bigint) as building_id
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
,cast(building_id as bigint) as building_id
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