create table if not exists db_dw_hr.clife_hr_dws_market_by_building_d_tmp stored as parquet as
select
split(getset2(concat(if(b1.building_id is null,'',b1.building_id),',',if(tt2.building_id is null,'',tt2.building_id),',',if(tt3.building_id is null,'',tt3.building_id),',',if(tt5.building_id is null,'',tt5.building_id),',',if(tt6.building_id is null,'',tt6.building_id),',',if(tt7.building_id is null,'',tt7.building_id),',',if(tt8.building_id is null,'',tt8.building_id),',',if(tt9.building_id is null,'',tt9.building_id),',',if(tt10.building_id is null,'',tt10.building_id),',',if(tt11.building_id is null,'',tt11.building_id),',','')),',')[1] as building_id
,split(getset2(concat(if(b1.sub_org_id is null,'',b1.sub_org_id),',',if(tt2.sub_org_id is null,'',tt2.sub_org_id),',',if(tt3.sub_org_id is null,'',tt3.sub_org_id),',',if(tt5.sub_org_id is null,'',tt5.sub_org_id),',',if(tt6.sub_org_id is null,'',tt6.sub_org_id),',',if(tt7.sub_org_id is null,'',tt7.sub_org_id),',',if(tt8.sub_org_id is null,'',tt8.sub_org_id),',',if(tt9.sub_org_id is null,'',tt9.sub_org_id),',',if(tt10.sub_org_id is null,'',tt10.sub_org_id),',',if(tt11.sub_org_id is null,'',tt11.sub_org_id),',','')),',')[1] as sub_org_id                          --   '养老机构id',
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
,tt11.contract_time_1_num_day            -- '合同有效期1年'
,tt11.contract_time_2_num_day            -- '合同有效期2年'
,tt11.contract_time_3_num_day            -- '合同有效期3年'
,tt11.contract_time_4_num_day            -- '合同有效期4年'
,tt11.contract_time_5_num_day            -- '合同有效期5年'
,tt11.contract_time_5_more_num_day       -- '合同有效期大于5年'
,split(getset2(concat(if(b1.regist_date is null,'',b1.regist_date),',',if(tt2.deposit_date is null,'',tt2.deposit_date),',',if(tt3.sign_date is null,'',tt3.sign_date),',',if(tt5.visit_date is null,'',tt5.visit_date),',',if(tt6.assess_date is null,'',tt6.assess_date),',',if(tt7.apply_date is null,'',tt7.apply_date),',',if(tt8.transfer_to_date is null,'',tt8.transfer_to_date),',',if(tt9.transfer_from_date is null,'',tt9.transfer_from_date),',',if(tt10.withdraw_date is null,'',tt10.withdraw_date),',',if(tt11.sign_date is null,'',tt11.sign_date),',','')),',')[1]  as data_date                  -- 数据日期
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
,from_unixtime( cast(b.regist_time as int),'yyyy-mm-dd')  as regist_date
,cast(building_id as bigint) building_id
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
,from_unixtime( cast(deposit_time as int),'yyyy-mm-dd')  as deposit_date
,t.membership_id
,t.room_id
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
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
,from_unixtime( cast(sign_time as int),'yyyy-mm-dd')  as sign_date
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
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
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
,cast(building_id as bigint) building_id
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
,cast(building_id as bigint) building_id
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
,from_unixtime( cast(t.apply_time as int),'yyyy-mm-dd')  apply_date
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
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
,from_unixtime( cast(t.transfor_to_time as int),'yyyy-mm-dd')  transfer_to_date
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
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
,from_unixtime( cast(t.transfor_from_time as int),'yyyy-mm-dd')  transfer_from_date
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
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
,from_unixtime( cast(t.withdraw_time as int),'yyyy-mm-dd')  withdraw_date
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
)  aa10
group by aa10.sub_org_id,aa10.building_id,aa10.withdraw_date
) tt10
on b1.sub_org_id=tt10.sub_org_id and b1.building_id=tt10.building_id and b1.regist_date=tt10.withdraw_date

full join
(
select
t1.sub_org_id
,t1.sign_date
,t1.building_id
,sum(contract_time_1_num_day      ) as contract_time_1_num_day         -- '合同有效期1年'
,sum(contract_time_2_num_day      ) as contract_time_2_num_day         -- '合同有效期2年'
,sum(contract_time_3_num_day      ) as contract_time_3_num_day         -- '合同有效期3年'
,sum(contract_time_4_num_day      ) as contract_time_4_num_day         -- '合同有效期4年'
,sum(contract_time_5_num_day      ) as contract_time_5_num_day         -- '合同有效期5年'
,sum(contract_time_5_more_num_day ) as contract_time_5_more_num_day    -- '合同有效期大于5年'
from
(
select
c.sub_org_id
,c.building_id
,from_unixtime( cast(c.sign_time as int),'yyyy-mm-dd')  sign_date
,case when c.card_valid=1 then 1 else 0 end as contract_time_1_num_day        -- '合同有效期1年'
,case when c.card_valid=2 then 1 else 0 end as contract_time_2_num_day        -- '合同有效期2年'
,case when c.card_valid=3 then 1 else 0 end as contract_time_3_num_day        -- '合同有效期3年'
,case when c.card_valid=4 then 1 else 0 end as contract_time_4_num_day        -- '合同有效期4年'
,case when c.card_valid=5 then 1 else 0 end as contract_time_5_num_day        -- '合同有效期5年'
,case when c.card_valid>5 then 1 else 0 end as contract_time_5_more_num_day   -- '合同有效期大于5年'
from
(select
 t.sign_id
,max(t.building_id) building_id
,max(t.sub_org_id )  sub_org_id                      --  '养老机构id'
,max(t.card_valid)   card_valid
,max(t.sign_time)    sign_time
from
db_dw_hr.clife_hr_dwd_market t
where t.part_date=${system.biz.date} and (customer_type=1 or customer_type is null)
group by t.sign_id)  c
) t1
group by t1.sub_org_id, t1.building_id, t1.sign_date) tt11
on b1.sub_org_id=tt11.sub_org_id and b1.building_id=tt11.building_id and b1.regist_date = tt11.sign_date
