set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

/**
db_dw_hr.clife_hr_dwd_market where customer_type=1  -- crm系统里面的数据，select 各种 money
 */
create table if not exists db_dw_hr.clife_hr_dwm_market_tmp stored as parquet as
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
,case when  from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd')=date_sub(current_date(),1) then t.received_amount then else null end as today_received_amount            --  '今日已收金额'
from db_dw_hr.clife_hr_dwd_market t
where part_date=regexp_replace(date_sub(current_date(),1),'-','') and customer_type=1;

/**
db_dw_hr.clife_hr_dwm_market_tmp a
sum(money) group by building_id, membership_adviser_id, room_type, product_id, sub_org_id
 */
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
db_dw_hr.clife_hr_dwm_market_tmp a
group by a.building_id,a.membership_adviser_id,a.room_type,a.product_id,a.sub_org_id;