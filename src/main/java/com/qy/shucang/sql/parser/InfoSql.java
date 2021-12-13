package com.qy.shucang.sql.parser;

public class InfoSql {
    public static String line = "select count(1) from (\n" +
            "select\n" +
            "b1.room_id as room_id      --'房间id'\n" +
            ",b1.sub_org_id as sub_org_id                          --   '养老机构id',\n" +
            ",b1.sub_org_name as sub_org_name                        --   '养老机构名称'\n" +
            ",b1.org_id as org_id                              --   '养老集团id'\n" +
            ",b1.org_id as org_name                            --   '养老集团名称'\n" +
            ",b1.area_id as area_id                             --   '区域id'\n" +
            ",b1.area_name as area_name                           --   '区域名称'\n" +
            ",b1.crm_first_intent_high_cnt           --   '营销管理模块首次意向感兴趣客户数'\n" +
            ",b1.crm_first_intent_mid_cnt            --   '营销管理模块首次意向兴趣一般客户数'\n" +
            ",b1.crm_first_intent_low_cnt            --   '营销管理模块首次意向不感兴趣客户数'\n" +
            ",b1.crm_first_intent_highest_cnt        --   '营销管理模块首次意向希望购买客户数'\n" +
            ",b1.crm_intent_high_cnt                 --   '营销管理模块当前意向感兴趣客户数'\n" +
            ",b1.crm_intent_mid_cnt                  --   '营销管理模块当前意向兴趣一般客户数'\n" +
            ",b1.crm_intent_low_cnt                  --   '营销管理模块当前意向不感兴趣客户数'\n" +
            ",b1.crm_intent_highest_cnt              --   '营销管理模块当前意向希望购买客户数'\n" +
            ",b1.crm_intent_cnt                      --   '营销管理模块意向客户数'\n" +
            ",b1.deposit_cust_cnt                    --   '下定客户数'\n" +
            ",b1.sign_cust_cnt                       --   '签约客户数'\n" +
            ",c1.transfer_to_member_cnt              --   '转让会籍数'\n" +
            ",c1.transfer_from_member_cnt            --   '继承会籍数'\n" +
            ",c1.withdraw_member_cnt                 --   '退会会籍数'\n" +
            ",b1.crm_intent_cust_visit_cnt           --   '营销管理模块意向客户回访次数'\n" +
            ",b1.deposit_cust_visit_cnt              --   '下定客户回访次数'\n" +
            ",b1.sign_cust_vist_cnt as sign_cust_visit_cnt                 --   '签约客户回访次数'\n" +
            ",b1.avg_crm_intent_cust_visit_cnt       --   '营销管理模块意向客户平均回访次数'\n" +
            ",b1.avg_deposit_cust_visit_cnt          --   '下定客户平均回访次数'\n" +
            ",b1.avg_sign_cust_visit_cnt             --   '签约客户平均回访次数'\n" +
            ",c1.apply_member_cnt                    --   '预申请会籍数量'\n" +
            ",c1.deposit_member_cnt                  --   '下定会籍数量'\n" +
            ",d1.deposit_room_cnt                    --   '下定房间数量'\n" +
            ",c1.sign_member_cnt                     --   '签约会籍数量'\n" +
            ",d1.sign_room_cnt                       --   '签约房间数量'\n" +
            ",e1.deposit_amount                      --   '下定额'\n" +
            ",e1.sales_amount                        --   '销售额'\n" +
            ",e1.card_pay_amount                     --      '刷卡金额'\n" +
            ",e1.cash_pay_amount                     --   '现金金额'\n" +
            ",e1.weixin_pay_amount                   --   '微信支付金额'\n" +
            ",e1.alipay_amount                       --   '阿里支付金额'    \n" +
            ",e1.other_pay_amount                    --   '其他方式支付金额'\n" +
            ",e1.received_amount                     --   '已收金额'\n" +
            ",e1.received_installment_amount         --   '已收分期金额'\n" +
            ",e1.received_occupancy_amount           --   '已收占用费'\n" +
            ",e1.membership_amount                   --   '会籍费'\n" +
            ",e1.overdue_amount                      --   '滞纳金'\n" +
            ",e1.received_overdue_amount             --   '已收滞纳金'\n" +
            ",e1.other_amount                        --   '其他费用'\n" +
            ",e1.manager_amount                      --   '管理费'\n" +
            ",e1.trust_amount                        --   '托管费' \n" +
            ",e1.membership_discount_amount          --   '会籍优惠价' \n" +
            ",e1.card_amount                         --   '卡费'\n" +
            ",e1.card_manager_amount                 --   '卡管理费'\n" +
            ",e1.other_transfor_to_amount            --   '其他转让费' \n" +
            ",e1.transfor_to_amount                  --   '转让费'\n" +
            ",e1.other_transfor_from_amount          --   '其他继承费' \n" +
            ",e1.transfor_from_amount                --   '继承费'\n" +
            ",e1.withdraw_amount                     --   '退会金额' \n" +
            ",b1.assess_cust_cnt  as assess_cust_cnt                     --   '评估客户数'\n" +
            ",b1.assess_cnt  as assess_cnt                          --   '评估总次数'\n" +
            ",regexp_replace(date_sub(current_date(),1),'-','')  as part_date\n" +
            "from \n" +
            "(\n" +
            " select\n" +
            " t1.sub_org_id\n" +
            ",t1.room_id\n" +
            ",max(t1.sub_org_name) sub_org_name\n" +
            ",max(t1.org_id)    org_id\n" +
            ",max(t1.org_name)    org_name\n" +
            ",max(t1.area_id)     area_id\n" +
            ",max(t1.area_name)   area_name\n" +
            ",sum(first_high_cnt)  as crm_first_intent_high_cnt  --'营销管理模块首次意向感兴趣客户数'\n" +
            ",sum(first_mid_cnt)   as crm_first_intent_mid_cnt   --'营销管理模块首次意向兴趣一般客户数'\n" +
            ",sum(first_low_cnt)   as crm_first_intent_low_cnt   --'营销管理模块首次意向不感兴趣客户数'\n" +
            ",sum(first_highest_cnt)  as crm_first_intent_highest_cnt  --'营销管理模块首次意向希望购买客户数'\n" +
            ",sum(high_cnt)     as crm_intent_high_cnt  --'营销管理模块意向感兴趣客户数'\n" +
            ",sum(mid_cnt)      as crm_intent_mid_cnt   --'营销管理模块意向兴趣一般客户数'\n" +
            ",sum(low_cnt)      as crm_intent_low_cnt   --'营销管理模块意向不感兴趣客户数'\n" +
            ",sum(highest_cnt)  as crm_intent_highest_cnt  --'营销管理模块意向希望购买客户数'\n" +
            ",count(distinct intention_customer_id)  as crm_intent_cnt    --'营销管理模块意向客户数',\n" +
            ",sum(t1.visit_cnt)   as crm_intent_cust_visit_cnt  -- '营销管理模块意向客户回访次数'\n" +
            ",sum(t1.visit_cnt)/count(distinct intention_customer_id)  as avg_crm_intent_cust_visit_cnt  --'营销管理模块意向客户平均回访次数'\n" +
            ",sum(deposit_cust_cnt) as deposit_cust_cnt   --'下定客户数'\n" +
            ",sum(sign_cust_cnt)  as sign_cust_cnt        --'签约客户数'\n" +
            ",sum(deposit_cust_visit_cnt)   as deposit_cust_visit_cnt  --'下定客户回访总次数'\n" +
            ",sum(sign_cust_vist_cnt)   as   sign_cust_vist_cnt    -- '签约客户回访总次数'\n" +
            ",sum(deposit_cust_visit_cnt)/sum(deposit_cust_cnt)  as avg_deposit_cust_visit_cnt  --'下定客户平均回访次数'\n" +
            ",sum(sign_cust_vist_cnt)/sum(sign_cust_cnt)  as avg_sign_cust_visit_cnt  --'签约客户平均回访次数'\n" +
            ",sum(assess_cnt) assess_cnt\n" +
            ",sum(assess_cust_cnt)  assess_cust_cnt\n" +
            " from \n" +
            "(\n" +
            "select \n" +
            " b.intention_customer_id\n" +
            ",b.sub_org_id\n" +
            ",b.sub_org_name\n" +
            ",b.org_id\n" +
            ",b.org_name\n" +
            ",b.area_id\n" +
            ",b.area_name\n" +
            ",b.visit_cnt\n" +
            ",b.assess_cnt\n" +
            ",b.room_id\n" +
            ",case when b.first_intention_type=1 then 1 else 0 end as first_high_cnt\n" +
            ",case when b.first_intention_type=2 then 1 else 0 end as first_mid_cnt\n" +
            ",case when b.first_intention_type=3 then 1 else 0 end as first_low_cnt\n" +
            ",case when b.first_intention_type=4 then 1 else 0 end as first_highest_cnt\n" +
            ",case when b.intention_type=1 then 1 else 0 end as high_cnt\n" +
            ",case when b.intention_type=2 then 1 else 0 end as mid_cnt\n" +
            ",case when b.intention_type=3 then 1 else 0 end as low_cnt\n" +
            ",case when b.intention_type=4 then 1 else 0 end as highest_cnt\n" +
            ",case when b.deposit_time is not null and b.deposit_time != '' then 1 else 0 end as deposit_cust_cnt\n" +
            ",case when b.sign_time is not null and b.sign_time != '' then 1 else 0 end as sign_cust_cnt\n" +
            ",case when b.deposit_time is not null and b.deposit_time != '' then b.visit_cnt else 0 end as deposit_cust_visit_cnt\n" +
            ",case when b.sign_time is not null and b.sign_time != '' then b.visit_cnt else 0 end as sign_cust_vist_cnt\n" +
            ",case when b.assess_cnt is not null then 1 else 0 end as assess_cust_cnt\n" +
            "from \n" +
            "(\n" +
            "select \n" +
            " t.intention_customer_id\n" +
            ",t.sub_org_id                      --  '养老机构id'\n" +
            ",t.sub_org_name                    --  '养老机构名称'\n" +
            ",t.org_id                      --  '集团id'\n" +
            ",t.org_name                       --  '集团名称'\n" +
            ",t.area_id                       --  '区域id'\n" +
            ",t.area_name                       --  '区域名称'\n" +
            ",t.visit_cnt                       --  '回访次数'\n" +
            ",t.first_intention_type           --  '首次意向'\n" +
            ",t.intention_type           --  '当前意向' \n" +
            ",t.assess_cnt                       \n" +
            ",t.deposit_time      -- '下定时间集合'\n" +
            ",t.sign_time       -- '签约时间集合'\n" +
            ",l1.room_id \n" +
            "from db_dw_hr.clife_hr_dws_crm_cust_room_info_tmp t \n" +
            "lateral view outer explode(split(room_ids,','))   l1 as  room_id \n" +
            ") b\n" +
            ") t1\n" +
            "group by t1.sub_org_id,t1.room_id) b1 \n" +
            "full join \n" +
            "(\n" +
            "select\n" +
            "t1.sub_org_id\n" +
            ",t1.room_id\n" +
            ",sum(t1.apply_member_cnt)  apply_member_cnt\n" +
            ",sum(t1.deposit_member_cnt)  deposit_member_cnt\n" +
            ",sum(t1.sign_member_cnt)  sign_member_cnt\n" +
            ",sum(t1.transfer_to_member_cnt)   transfer_to_member_cnt\n" +
            ",sum(t1.transfer_from_member_cnt)  transfer_from_member_cnt\n" +
            ",sum(t1.withdraw_member_cnt)   withdraw_member_cnt\n" +
            "from \n" +
            "(\n" +
            "select \n" +
            "c.membership_id\n" +
            ",c.sub_org_id\n" +
            ",c.room_id\n" +
            ",case when apply_time   is not null and apply_time != '' then 1 else 0 end as apply_member_cnt\n" +
            ",case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_member_cnt\n" +
            ",case when sign_time   is not null and sign_time != '' then 1 else 0 end as sign_member_cnt\n" +
            ",case when transfer_to_time is not null and transfer_to_time != '' then 1 else 0 end as transfer_to_member_cnt\n" +
            ",case when transfer_from_time is not null and transfer_from_time != '' then 1 else 0 end as transfer_from_member_cnt\n" +
            ",case when withdraw_time is not null and withdraw_time != '' then 1 else 0 end as withdraw_member_cnt\n" +
            "from \n" +
            "(select \n" +
            " t.membership_id  \n" +
            ",t.sub_org_id                      --  '养老机构id'\n" +
            ",room_id\n" +
            ",apply_time      -- '预申请时间集合' \n" +
            ",deposit_time      -- '下定时间集合'\n" +
            ",sign_time       -- '签约时间集合'\n" +
            ",transfer_to_time       -- '转让时间集合'\n" +
            ",transfer_from_time       -- '继承时间集合'\n" +
            ",withdraw_time       -- '退会时间集合'\n" +
            "from \n" +
            "db_dw_hr.clife_hr_dws_crm_member_room_info_tmp  t \n" +
            "lateral view outer explode(split(room_ids,','))   l1 as  room_id )  c\n" +
            ") t1\n" +
            "group by t1.sub_org_id,t1.room_id) c1\n" +
            "on b1.sub_org_id=c1.sub_org_id  and b1.room_id=c1.room_id\n" +
            "left join \n" +
            "(\n" +
            "select\n" +
            "t1.sub_org_id\n" +
            ",t1.room_id\n" +
            ",sum(t1.deposit_room_cnt)  deposit_room_cnt\n" +
            ",sum(t1.sign_room_cnt)  sign_room_cnt\n" +
            "from \n" +
            "(\n" +
            "select \n" +
            "c.room_id\n" +
            ",c.sub_org_id\n" +
            ",case when deposit_time is not null and deposit_time != '' then 1 else 0 end as deposit_room_cnt\n" +
            ",case when sign_time   is not null and sign_time != '' then 1 else 0 end as sign_room_cnt\n" +
            "from \n" +
            "(select \n" +
            " t.room_id  \n" +
            ",max(t.sub_org_id )  sub_org_id                      --  '养老机构id' \n" +
            ",concat_ws(',',collect_list(from_unixtime( cast(t.deposit_time as int),'yyyy-MM-dd  HH:mm:ss')))   as deposit_time      -- '下定时间集合'\n" +
            ",concat_ws(',',collect_list(from_unixtime( cast(t.sign_time as int),'yyyy-MM-dd  HH:mm:ss')))  as sign_time       -- '签约时间集合'\n" +
            "from \n" +
            "db_dw_hr.clife_hr_dwd_market t \n" +
            "where t.part_date=regexp_replace(date_sub(current_date(),1),'-','') and (customer_type=1 or customer_type is null)\n" +
            "group by t.room_id)  c\n" +
            ") t1\n" +
            "group by t1.sub_org_id,t1.room_id) d1\n" +
            "on b1.sub_org_id=d1.sub_org_id  and b1.room_id=d1.room_id\n" +
            "left join \n" +
            "(select \n" +
            " sub_org_id\n" +
            ",room_id\n" +
            ",sum(deposit_amount)    deposit_amount         --     '下定额'\n" +
            ",sum(card_pay_amount)   card_pay_amount         --        '刷卡金额'\n" +
            ",sum(cash_pay_amount)   cash_pay_amount         --     '现金金额'\n" +
            ",sum(weixin_pay_amount) weixin_pay_amount         --     '微信支付金额'\n" +
            ",sum(alipay_amount)     alipay_amount         --     '阿里支付金额'    \n" +
            ",sum(other_pay_amount)  other_pay_amount         --     '其他方式支付金额'\n" +
            ",sum(receivable_amount) sales_amount            --  '应收金额'\n" +
            ",sum(received_amount)   received_amount            --  '已收金额'\n" +
            ",sum(received_installment_amount) received_installment_amount    --  '已收分期金额'\n" +
            ",sum(received_occupancy_amount)   received_occupancy_amount    --  '已收占用费'\n" +
            ",sum(membership_amount)           membership_amount     --  '会籍费'\n" +
            ",sum(overdue_amount)              overdue_amount    --  '滞纳金'\n" +
            ",sum(received_overdue_amount)     received_overdue_amount    --  '已收滞纳金'\n" +
            ",sum(other_amount)                other_amount     --  '其他费用'\n" +
            ",sum(manager_amount)              manager_amount       --  '管理费'\n" +
            ",sum(trust_amount)                trust_amount      --  '托管费'\n" +
            ",sum(membership_discount_amount)  membership_discount_amount      --  '会籍优惠价'  \n" +
            ",sum(card_amount)                 card_amount        --  '卡费'\n" +
            ",sum(card_manager_amount)         card_manager_amount     --  '卡管理费'\n" +
            ",sum(other_transfor_to_amount)    other_transfor_to_amount     --  '其他转让费' \n" +
            ",sum(transfor_to_amount)          transfor_to_amount      --  '转让费'\n" +
            ",sum(other_transfor_from_amount)  other_transfor_from_amount    --  '其他继承费'\n" +
            ",sum(transfor_from_amount)        transfor_from_amount       --  '继承费'\n" +
            ",sum(withdraw_amount)             withdraw_amount        --  '退会金额'\n" +
            "from db_dw_hr.clife_hr_dwm_market where part_date= regexp_replace(date_sub(current_date(),1),'-','')\n" +
            "group by sub_org_id,room_id\n" +
            ") e1\n" +
            "on b1.sub_org_id=e1.sub_org_id  and b1.room_id=e1.room_id\n" +
            "\n" +
            "\n" +
            ") adf";
}
