================================================================================
==========          clife_hr_ods_intentional_customer                 ==========
================================================================================
select 
     customer_id             
    ,create_time             
    ,create_user_id          
    ,group_id                
    ,org_id                  
    ,sub_org_id              
    ,name                    
    ,sex_option              
    ,birthday                
    ,phone                   
    ,age_option              
    ,credent_type            
    ,id_card_no              
    ,province                
    ,city                    
    ,area                    
    ,town                    
    ,address                 
    ,trade_channel_types     
    ,trade_channel_ids       
    ,intentional_type        
    ,membership_adviser_id   
    ,visit_count             
    ,return_visit_remind_time
    ,communication_content   
    ,remark                  
    ,visit_type              
    ,filling_time            
    ,questionnaire_id        
    ,del_flag                
    ,membership_id           
    ,membership_name         
    ,status                  
    ,phone_visit             
    ,receive_status          
    ,apartment_type          
    ,room_type               
    ,vitality_intention      
    ,customer_lv             
    ,consult_time            
    ,possible_customer       
    ,cost_type               
    ,age                     
    ,retire_worker           
    ,hobbies                 
    ,about_org               
    ,resistance 
from db_nursing_crm.tb_intentional_customer 
where str_to_date(create_time,'%Y-%m-%d') <str_to_date(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_advisory_customer                    ==========
================================================================================
select 
     advisory_customer_id  
    ,customer_name         
    ,phone_number          
    ,intentional_type      
    ,sex                   
    ,reception_date        
    ,sales_consultant_id   
    ,sales_consultant_name 
    ,visit_count           
    ,linked_customer_num   
    ,create_time           
    ,update_time           
    ,create_user_id        
    ,org_id                
    ,sub_org_id            
    ,status
from db_pension_organization.tb_advisory_customer
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_intentional_customer_visit           ==========
================================================================================
select 
     visit_id                 
    ,create_time              
    ,create_user_id           
    ,create_user_name         
    ,org_id                   
    ,sub_org_id               
    ,customer_id              
    ,membership_id            
    ,membership_name          
    ,intentional_type         
    ,return_visit_remind_time 
    ,visit_date               
    ,visit_type               
    ,content                  
    ,remark
from db_nursing_crm.tb_intentional_customer_visit
where DATE_FORMAT(create_time,'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_account                              ==========
================================================================================
select
 account_id
,account_serial_number
,account_template_id
,old_man_id
,balance
,org_id
,sub_org_id
,create_time
,update_time
,state
,account_name
,attributes
,symbol
from db_pension_organization.tb_hr_account 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_account_fee_setting                  ==========
================================================================================
select
 account_fee_setting_id
,account_template_id
,fee_item_id
,org_id
,sub_org_id
,create_time
,update_time
,state
from db_pension_organization.tb_hr_account_fee_setting 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_account_serial_number                ==========
================================================================================
select
 id
,account_serial_number
,old_man_id
,org_id
,sub_org_id
,create_time
,status
from db_pension_organization.tb_hr_account_serial_number 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_account_template                     ==========
================================================================================
select
 account_template_id
,account_name
,attributes
,symbol
,description
,org_id
,sub_org_id
,state
,create_time
,update_time
,default_recharge_amount
,default_notice_amount
from db_pension_organization.tb_hr_account_template 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_assess_item                          ==========
================================================================================
select
     assess_id   
    ,parent_id   
    ,score       
    ,type        
    ,name        
    ,des         
    ,level       
    ,single      
    ,create_time
from db_nursing_home.tb_assess_item 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_assess_item_score                    ==========
================================================================================
select
     score_id   
    ,score      
    ,assess_id  
    ,old_man_id 
    ,sub_org_id 
    ,org_id     
    ,id_card_no 
    ,admin_id   
    ,level      
    ,create_time
    ,state
from db_nursing_home.tb_assess_item_score 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_assess_item_selected                 ==========
================================================================================
select
     select_id  
    ,assess_id  
    ,score_id   
    ,old_man_id 
    ,selected   
    ,org_id     
    ,admin_id   
    ,create_time
    ,others
from db_nursing_home.tb_assess_item_selected 
where date_format(create_time,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_assess_report                        ==========
================================================================================
select
 assess_report_id
,assess_report_no
,old_man_id
,assess_report_status
,assess_report_level
,illness_level
,old_man_level
,nursing_level
,social_support_level
,illness_level_score_id
,old_man_level_score_id
,nursing_level_score_id
,social_support_level_score_id
,create_time
,update_time
,org_id
,sub_org_id
,state
,syndrome_number
,syndrome_score_id
from db_nursing_home.tb_assess_report 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_basic_service_plan                   ==========
================================================================================
select
 plan_id
,service_project_id
,service_type_id
,service_frequency_status
,service_frequency
,service_object
,service_object_desc
,room_ids
,old_man_ids
,implementation_time
,every_flag
,service_keeper_ids
,supplier_id
,remark
,create_time
,update_time
,state
,org_id
,sub_org_id
,service_project_name
,service_type_name
,service_keeper_names
,create_user_id
,create_user_name
,old_man_check_in_id
,accurate_flag
from db_pension_organization.tb_basic_service_plan 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_company_account                      ==========
================================================================================
select
 company_account_id
,bank_name
,bank_card_id
,description
,create_time
,update_time
,org_id
,sub_org_id
,create_user_id
,status
from db_pension_organization.tb_hr_company_account 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_contract                             ==========
================================================================================
select
 contract_id
,contract_num
,type
,old_man_id
,start_date
,end_date
,create_time
,update_time
,state
,user_id
,sub_org_id
,org_id
,manager_id
,old_man_check_in_id
,renew_day
from db_pension_organization.tb_contract 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_contract_deposit                     ==========
================================================================================
select
     deposit_id    
    ,deposit_num   
    ,intent_user_id
    ,payee_name    
    ,payee_phone   
    ,membership_id 
    ,membership_name
    ,deposit_amount 
    ,membership_adviser_id
    ,house_type_id
    ,pay_amount   
    ,refund_amount
    ,deposit_status
    ,user_id    
    ,sub_org_id 
    ,org_id     
    ,remark     
    ,create_time
    ,update_time
    ,building_id
    ,floor_id
    ,room_id 
    ,bed_id 
    ,apply_id
    ,receipt_number
from db_nursing_crm.tb_contract_deposit 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_contract_files                       ==========
================================================================================
select
     file_id
    ,file_name
    ,file_path
    ,contract_id
    ,update_time
    ,create_time
from db_nursing_crm.tb_contract_files
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_contract_sign                        ==========
================================================================================
select
     sign_id          
    ,intent_user_id   
    ,contract_num     
    ,contract_status  
    ,membership_type  
    ,membership_name  
    ,membership_num   
    ,membership_amount
    ,chip_num         
    ,building_id      
    ,floor_id         
    ,room_id          
    ,bed_id           
    ,sign_house       
    ,originator_type  
    ,reason           
    ,is_pay_deposit   
    ,deposit_amount   
    ,deposit_num      
    ,received_fee     
    ,pay_state        
    ,sign_date        
    ,card_valid       
    ,house_type       
    ,other_amount     
    ,other_amount_explain
    ,is_divided          
    ,membership_effective_time 
    ,membership_id    
    ,product_id       
    ,user_id          
    ,sub_org_id       
    ,org_id           
    ,update_time      
    ,create_time      
    ,cancle_time      
    ,manager_fee      
    ,living_limit     
    ,discount         
    ,total_deduction 
    ,daily_deduction_limit     
    ,description   
    ,card_type     
    ,is_agency     
    ,agency_id     
    ,payment_type_id   
    ,payment_type_name 
    ,service_fee_id    
    ,del_flag          
    ,service_fee_amount
    ,service_fee_name  
    ,is_trust    
    ,trust_limit 
    ,trust_fee   
    ,trust_id    
    ,membership_discount
    ,apply_id       
    ,show_status    
    ,free_visit_day 
from db_nursing_crm.tb_contract_sign 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_countermand_pay_record               ==========
================================================================================
select
 id
,old_man_id
,fee_amount
,account_id
,account_name
,balance
,create_time
,state
,org_id
,sub_org_id
,update_time
,old_man_check_in_id
,countermand_pay_id
,attributes
from db_pension_organization.tb_hr_countermand_pay_record 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_coupon                               ==========
================================================================================
select
 id
,coupon_num
,user_type
,user_id
,user_name
,room_name
,coupon_setting_id
,type
,attributes
,apartment_name
,coupon_name
,coupon_amount
,period
,preferential_plan
,discount_fee_type
,discount_fee_id
,discount_fee_name
,valid_period_start
,valid_period_end
,create_time
,update_time
,org_id
,sub_org_id
,use_frequency
,balance
,use_name
,use_date
,use_status
,status
from db_pension_organization.tb_hr_coupon 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_coupon_issue                         ==========
================================================================================
select
 id
,coupon_setting_id
,issue_type
,issue_id_list
,issue_name_list
,issue_coupon_id_list
,type
,attributes
,apartment_name
,coupon_name
,coupon_amount
,period
,preferential_plan
,discount_fee_type
,discount_fee_id
,discount_fee_name
,valid_period_start
,valid_period_end
,issue_user_id
,issue_user_name
,create_time
,update_time
,org_id
,sub_org_id
,status
from db_pension_organization.tb_hr_coupon_issue 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_coupon_setting                       ==========
================================================================================
select
 id
,type
,attributes
,apartment_name
,coupon_name
,coupon_amount
,period
,preferential_plan
,discount_fee_type
,discount_fee_id
,discount_fee_name
,valid_period_start
,valid_period_end
,create_user_id
,create_user_name
,create_time
,update_time
,org_id
,sub_org_id
,status
from db_pension_organization.tb_hr_coupon_setting 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_customer_visit_record                ==========
================================================================================
select
 visit_id
,create_time
,update_time
,create_user_id
,create_user_name
,org_id
,sub_org_id
,customer_id
,intentional_type
,return_visit_remind_time
,visit_date
,visit_type
,content
,remark
,status
from db_pension_organization.tb_customer_visit_record
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_detail_record                        ==========
================================================================================
select
 fee_detail_record_id
,old_man_id
,fee_type_id
,fee_type_name
,fee_iteam_id
,fee_iteam_name
,fee_amount
,create_time
,state
,remark
,org_id
,sub_org_id
,update_time
,old_man_check_in_id
,visitor_id
,`type`
,sn
,child_id
,consumption_time
from db_pension_organization.tb_hr_fee_detail_record 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_discount_detail                      ==========
================================================================================
select
     discount_id
    ,sign_id
    ,discount_name
    ,discount_code
    ,type
    ,value
    ,update_time
    ,create_time
    ,is_show
    ,recharge_amount
    ,discount_amount
from db_nursing_crm.tb_discount_detail
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_execute_detail                       ==========
================================================================================
select
 execute_detail_id
,snap_item_id
,user_id
,state
,sub_org_id
,create_time
,update_time
,plan_id
from db_pension_organization.tb_sy_nurse_execute_detail 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_fee                                  ==========
================================================================================
select
     fee_id
    ,org_id
    ,sub_org_id
    ,number
    ,create_time
    ,update_time
    ,create_user_id
    ,fee_type
    ,type
    ,relation_id
    ,membership_name
    ,amount
    ,confirm_amount
    ,confirm_time
    ,confirm_user_id
    ,receive_type_option
    ,status
    ,del_flag
    ,source
    ,is_installment
    ,overdue_type
    ,overdue_amount
    ,model_status
from db_nursing_crm.tb_fee
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_fee_detail                           ==========
================================================================================
select
     detail_id
    ,fee_id
    ,create_time
    ,create_user_id
    ,name
    ,pre_receive_time
    ,confirm_time
    ,confirm_user_id
    ,amount
    ,receive_type_option
    ,status
    ,payment_detail_id
    ,overdue_fine
    ,confirm_amount
    ,deposits_amount
    ,stage_sum
    ,occupation_sum
    ,overdue_sum
    ,other_money_amount
from db_nursing_crm.tb_fee_detail
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_fee_item                             ==========
================================================================================
select
 fee_item_id
,fee_type_id
,fee_item_name
,property
,cost
,meal
,description
,org_id
,sub_org_id
,create_time
,update_time
,state
from db_pension_organization.tb_hr_fee_item 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_fee_type                             ==========
================================================================================
select
 fee_type_id
,fee_type
,create_time
,update_time
,create_user_id
,org_id
,sub_org_id
,description
,status
from db_pension_organization.tb_hr_fee_type 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_food                                 ==========
================================================================================
select
 food_id
,food_name
,food_type_id
,food_type_name
,food_price
,disease_taboo
,food_picture
,restaurant_id
,restaurant_name
,serving_time_id
,state
,org_id
,sub_org_id
,create_time
,update_time
,remark
from db_pension_organization.tb_hr_food 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_food_order                           ==========
================================================================================
select
 order_id
,user_id
,order_date
,old_man_name
,bed_name
,bed_id
,room_id
,order_food
,amount
,delivery_state
,settlement_state
,deliveries_id
,deliveries
,delivery_time
,state
,org_id
,sub_org_id
,create_time
,update_time
,old_man_id
,old_man_check_in_id
,restaurant_id
,serving_time_id
from db_pension_organization.tb_hr_food_order 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_food_type                            ==========
================================================================================
select
 food_type_id
,food_type_name
,remark
,org_id
,sub_org_id
,create_time
,update_time
,status
from db_pension_organization.tb_hr_food_type 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');



================================================================================
==========          clife_hr_ods_increase_service                     ==========
================================================================================
select
 increase_service_id
,service_item_id
,service_item_name
,service_type_id
,service_type_name
,status
,attributes
,money
,apartment_money
,nursing_money
,units
,pic_url
,service_housekeeper_ids
,service_housekeeper
,suplier_id
,suplier_name
,sub_org_id
,org_id
,create_time
,update_time
,create_user_id
,update_user_id
,remark
from db_pension_organization.tb_increase_service 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_inform                               ==========
================================================================================
select
 hr_inform_id
,hr_inform_type_id
,old_man_id
,agent_id
,sum_cost_money
,handle_type_id
,handle_content
,handle_time
,org_id
,sub_org_id
,state
,create_time
,update_time
,old_man_check_in_id
,agent_name
,source_code
,ignores
from db_pension_organization.tb_hr_inform 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_intention_customer                   ==========
================================================================================
select
 intention_customer_id
,old_man_id
,customer_name
,sex
,phone_num
,credent_type
,id_card_num
,birthday
,reception_date
,intentional_type
,sales_consultant_id
,sales_consultant_name
,linked_advisory_customer_id
,visit_count
,guardian_name
,guardian_sex
,relationship
,guardian_phone_num
,guardian_credent_type
,guardian_id_card_num
,remark
,care_level
,assess_status
,create_time
,create_user_id
,update_time
,sub_org_id
,org_id
,status
from db_pension_organization.tb_intention_customer
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_account                       ==========
================================================================================
select
 id
,member_name
,card_type
,card_number
,phone
,amount
,member_type
,member_id
,org_id
,sub_org_id
,state
,create_time
,update_time
,remark
,member_status
,member_time
from db_pension_organization.tb_hr_member_account 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_debit_record                  ==========
================================================================================
select
 debit_record_id
,member_id
,payee_id
,old_man_check_in_id
,real_pay_amount
,coupon_pay_amount
,state
,org_id
,sub_org_id
,type
,create_time
,update_time
,membership_num
,coupon_id
from db_pension_organization.tb_hr_member_debit_record 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_month_bill                    ==========
================================================================================
select
id
,member_id
,total_amount
,bill_time
,bill_status
,state
,org_id
,sub_org_id
,create_time
,update_time
from db_pension_organization.tb_hr_member_month_bill 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_month_bill_child              ==========
================================================================================
select
id
,member_id
,old_man_id
,old_man_check_in_id
,fee_amount
,state
,remark
,org_id
,sub_org_id
,fee_status
,month_bill_id
,create_time
,update_time
,membership_num
from db_pension_organization.tb_hr_member_month_bill_child 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_setting_cost                  ==========
================================================================================
select
 id
,membership_num
,man_id
,fee_relation_id
,cost
,state
,org_id
,sub_org_id
,type
,people_type
,create_time
,update_time
from db_pension_organization.tb_hr_member_setting_cost 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_membership_owner                     ==========
================================================================================
select
     id
    ,contract_id
    ,name
    ,sex
    ,birthday
    ,phone
    ,id_card
    ,card_type
    ,age
    ,census_register
    ,census_province_id
    ,census_province_name
    ,census_city_id
    ,census_city_name
    ,census_area_id
    ,census_area_name
    ,residence
    ,residence_province_id
    ,residence_province_name
    ,residence_city_id
    ,residence_city_name
    ,residence_area_id
    ,residence_area_name
    ,unit
    ,title
    ,hobbies
    ,physical_condition
    ,disease_history
    ,create_time
    ,type
    ,user_id
    ,org_id
    ,sub_org_id
    ,status
    ,audit_user_id
from db_nursing_crm.tb_membership_owner
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_metro_card                           ==========
================================================================================
select
 card_id
,card_no
,status
,nursin_homes_id
,sub_org_id
,org_id
,create_time
,update_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
from db_pension_organization.tb_metro_card 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_metro_card_cashier                   ==========
================================================================================
select
 id
,uid
,jobNumber
,name
,tel
,enable
,create_time
,sub_org_id
from db_pension_organization.tb_metro_card_cashier 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');



================================================================================
==========          clife_hr_ods_metro_card_consumer                  ==========
================================================================================
select
 card_consumer_id
,card_no
,customer_user_id
,operate_type
,consumer_money
,consumer_time
,create_time
,nusring_home_id
,sub_org_id
,org_id
,cashier_uid
,sn
,user_id
from db_pension_organization.tb_metro_card_consumer 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_metro_card_product                   ==========
================================================================================
select
 id
,uid
,parentUid
,sub_org_id
,create_time
,name
from db_pension_organization.tb_metro_card_product_category 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_metro_card_recharge                  ==========
================================================================================
select
 card_recharge_id
,card_no
,customer_user_id
,card_user_name
,user_type
,recharge_money
,gift_money
,recharge_time
,create_time
,nursing_home_id
,sub_org_id
,org_id
,balance
,user_id
from db_pension_organization.tb_metro_card_recharge 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');



================================================================================
==========          clife_hr_ods_metro_card_statement                 ==========
================================================================================
select
 card_consumer_statement_id
,card_no
,sn
,customer_user_id
,name
,quantity
,total_amount
,create_time
,nusring_home_id
,sub_org_id
,org_id
,product_uid
,category_uid
,cashier_uid
,user_id
,operate_type
from db_pension_organization.tb_metro_card_consumer_statement 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_metro_card_user                      ==========
================================================================================
select
 card_user_id
,card_no
,user_id
,user_name
,card_user_no
,birth_date
,customer_uid
,user_type
,door_permission
,consumer_permission
,contact_number
,room_name
,sex
,balance
,recharge_money
,gift_money
,status
,nursin_homes_id
,sub_org_id
,org_id
,create_time
,update_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,old_man_check_in_id
,enables
,is_binding
from db_pension_organization.tb_metro_card_user 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_nurse_plan_snapshot                  ==========
================================================================================
select
 snap_item_id
,user_id
,sub_org_id
,project_id
,project_name
,project_type_id
,project_type_name
,frequency_type
,frequency_num
,count
,create_time
,update_time
,exec_date
,plan_id
,complete_stase
from db_pension_organization.tb_sy_nurse_plan_snapshot 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_nurse_project_type                   ==========
================================================================================
select
 project_type_id
,project_type_name
,state
,sub_org_id
,create_time
,update_time
from db_pension_organization.tb_sy_nurse_project_type 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_old_man_reside                       ==========
================================================================================
select
 old_man_reside_id
,old_man_id
,membership_owner_id
,old_man_name
,old_man_type
,relation_id
,house_type
,house_type_name
,room_name
,room_area
,room_attribute
,building_id
,floor_id
,room_id
,bed_id
,nursin_homes_id
,membership_num
,card_type
,card_num
,sex
,birthday
,phone
,contact_information
,native_place
,reside_status
,reside_type
,reside_type_name
,estimated
,nursing_level_id
,nursing_level
,deposited
,deposit_amount
,reside_time
,reside_over_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,org_id
,sub_org_id
,status
,create_time
,update_time
,charge_type
from db_pension_organization.tb_old_man_reside 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_pay_type                             ==========
================================================================================
select
 id
,pay_type_name
,state
,org_id
,sub_org_id
,create_time
,remark
from db_pension_organization.tb_hr_pay_type 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_payment_detail                       ==========
================================================================================
select
     payment_detail_id
    ,sign_id
    ,pay_amount
    ,occupancy_amount
    ,dead_line
    ,`sort`
    ,pay_rate
    ,pay_days
    ,is_occupancy
    ,occupancy_rate
    ,occupancy_days
    ,del_flag
    ,org_id
    ,sub_org_id
    ,create_time
    ,update_time
    ,default_scale
    ,overdue
from db_nursing_crm.tb_payment_detail
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_recharge_record                      ==========
================================================================================
select
 id
,old_man_id
,fee_amount
,pay_type_id
,org_id
,sub_org_id
,create_time
,state
,remark
,payee
,payee_id
,update_time
,old_man_check_in_id
,account_id
,child_fee
,company_account_id
,recharge_number
,receipt_number
,pay_time
from db_pension_organization.tb_hr_recharge_record 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_relatives_visit                      ==========
================================================================================
select
     id
    ,old_man_id
    ,old_man_name
    ,old_man_room_name
    ,membership_number
    ,relatives_name
    ,sex
    ,phone
    ,card_type
    ,card_number
    ,visit_start_time
    ,visit_end_time
    ,visit_status
    ,visit_relation
    ,create_worker_id
    ,create_worker_name
    ,remark
    ,org_id
    ,sub_org_id
    ,state
    ,create_time
    ,update_time
    ,ex_id
from db_pension_organization.tb_relatives_visit 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_reside_fee_setting                   ==========
================================================================================
select
 reside_fee_setting_id
,fee_item_id
,reside_type
,org_id
,sub_org_id
,create_time
,update_time
,state
from db_pension_organization.tb_hr_reside_fee_setting 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_service_item                         ==========
================================================================================
select
 service_item_id
,service_item_name
,service_type_id
,service_type_name
,service_status
,money
,units
,nursin_homes_id
,sub_org_id
,org_id
,status
,remark
,create_time
,update_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,apartment_money
,nursing_money
,attributes
from db_pension_organization.tb_organization_service_item 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_service_record                       ==========
================================================================================
select
 increment_service_id
,old_man_name
,old_man_id
,service_type_name
,service_type_id
,service_project_id
,service_project_name
,service_type
,cost
,company
,number
,remarks
,total_cost
,settlement_status
,service_status
,service_time
,suplier_id
,suplier_name
,create_time
,update_time
,state
,org_id
,sub_org_id
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,bed_id
,bed_name
,room_id
,room_name
,building_id
,building_name
,service_housekeeper_ids
,service_housekeeper
,third_flag
,old_man_check_in_id
,pic_url
,service_object
,floor_id
,floor_name
,service_finish_time
,evaluation_status
,valid_status
,time_stage
,is_accurate
,plan_id
,ignore_status
from db_pension_organization.tb_increment_service_record 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_service_type                         ==========
================================================================================
select
 service_type_id
,service_type_name
,service_status
,nursin_homes_id
,sub_org_id
,org_id
,status
,remark
,create_time
,update_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
from db_pension_organization.tb_organization_service_item 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_sy_nurse_plan                        ==========
================================================================================
select
     item_id
    ,user_id
    ,sub_org_id
    ,project_id
    ,project_name
    ,project_type_id
    ,project_type_name
    ,frequency_type
    ,frequency_num
    ,create_time
from db_pension_organization.tb_sy_nurse_plan 
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_sy_nurse_programme                   ==========
================================================================================
select
 programme_id
,nurse_grade_id
,frequency_type
,frequency_num
,state
,sub_org_id
,create_time
,update_time
,project_id
,project_type_id
,project_name
,project_type_name
from db_pension_organization.tb_sy_nurse_programme 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_sy_nurse_project                     ==========
================================================================================
select
 project_id
,project_type_id
,project_name
,state
,sub_org_id
,create_time
,update_time
from db_pension_organization.tb_sy_nurse_project 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_water_electricity_price              ==========
================================================================================
select
id
,water_unit_price
,electricity_unit_price
,org_id
,sub_org_id
,create_time
,update_time
,status
from db_pension_organization.tb_hr_water_electricity_unit_price 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');



================================================================================
==========          clife_hr_ods_water_electricity_record             ==========
================================================================================
select
 record_id
,user_id
,room_id
,room_name
,old_man_ids
,old_man_names
,cost_item
,this_date
,last_date
,this_value
,last_value
,price
,update_user_id
,settle_status
,state
,create_time
,org_id
,sub_org_id
,update_time
,remark
from db_pension_organization.tb_water_electricity_record 
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');



================================================================================
==========          clife_hr_ods_old_man                              ==========
================================================================================
select
 old_man_id
,old_man_name
,id_card_no
,sex
,birthday
,lunar_birthday
,solar_birthday
,nurse_man_id
,need_nurse
,illness_case_ids
,location_id
,portrait_url
,phone
,contact_information
,old_man_startus
,nursin_homes_id
,create_time
,bed_id
,nurse_point_ids
,user_id
,admission_date
,checkin_date
,center_id
,nursing_level_id
,nursing_level
,service_scheme_id
,service_scheme_name
from db_nursing_home.tb_old_man
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_old_man_extend                       ==========
================================================================================
select
 old_man_id
,linkman
,linkman_phone
,nation
,social_security_card
,card_place
,card_hospital
,education_degree
,religion
,marital_status
,living_status
,medical_costs
,finance_source
,dementia
,mental_disease
,sickness_history
,credent_type
,native_address
,guardian
,guardian_contactInfo
,guardian_phone
,guardian_relationship
,guardian_email
,native_place
,live_address
,live_city
,nursing_level
,ability_level
,politic_countenance
,blood_type
,is_local_household_registration
,retirement_career
,birthday_type
,nursing_grade_id
,emergency_name
,emergency_relationship
,emergency_contactInfo
,province
,city
,area
,address
from db_nursing_home.tb_old_man_extend


================================================================================
==========          clife_hr_ods_contract_files                       ==========
================================================================================
select
     file_id
    ,file_name
    ,file_path
    ,contract_id
    ,update_time
    ,create_time
from db_nursing_crm.tb_contract_files
where DATE_FORMAT(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_worker_new                           ==========
================================================================================
select
     worker_id
    ,worker_name
    ,nurse_worker_id
    ,worker_role_id
    ,credent_type
    ,credent_number
    ,sex
    ,birthday
    ,natives_province
    ,natives_city
    ,native_address
    ,worker_type
    ,portrait_url
    ,phone
    ,org_id
    ,sub_org_id
    ,user_id
    ,worker_status
    ,nurse_home_id
    ,create_time
    ,update_time
    ,job_title
    ,center_id
    ,has_supplier
    ,personnel_type
    ,occupation_type
    ,position
    ,department_name
    ,department_id
    ,live_address
from db_csleep_account.tb_worker_new
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_check_in                             ==========
================================================================================
select
 old_man_check_in_id
,old_man_reside_id
,old_man_id
,contract_id
,membership_owner_id
,old_man_name
,old_man_type
,relation_id
,house_type
,house_type_name
,room_name
,room_area
,room_attribute
,building_id
,floor_id
,room_id
,bed_id
,nursin_homes_id
,membership_num
,card_type
,card_num
,sex
,birthday
,phone
,reside_status
,reside_type
,reside_type_name
,estimated
,nursing_level_id
,nursing_level
,deposited
,deposit_amount
,reside_time
,reside_over_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,org_id
,sub_org_id
,status
,create_time
,update_time
,charge_type
,check_in_type
,check_in_name
,agent_id
,agent_name
,guardian
,guardian_phone
,guardian_relationship
from db_pension_organization.tb_old_man_check_in
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_old_man_contract                     ==========
================================================================================
select
 contract_id
,pre_contract_id
,org_id
,sub_org_id
,create_time
,update_time
,state
,old_man_check_in_id
from db_pension_organization.tb_hr_old_man_contract
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_membership                           ==========
================================================================================
select
 id
,contract_id
,membership_status
,return_status
,return_amount
,return_amount_status
,return_date
,transfer_status
,user_id
,sub_org_id
,org_id
,update_time
,create_time
,overdue_type
,overdue_amount
from db_nursing_crm.tb_membership
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_location_room                        ==========
================================================================================
select
 room_id
,floor_id
,room_name
,create_time
,inside_room_types
,nursing_homes_id
,room_type
,house_type_id
,house_type_name
,orientation
,orientation_name
,room_attribute
,room_area
,sales_status
from db_nursing_home.tb_location_room
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_location_floor                       ==========
================================================================================
select
 floor_id
,building_id
,floor_name
,create_time
,nursing_homes_id
from db_nursing_home.tb_location_floor
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_location_bed                         ==========
================================================================================
select
 bed_id
,room_id
,bed_name
,create_time
,nursing_homes_id
,day_cost
,month_cost
,sales_status
from db_nursing_home.tb_location_bed
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_location_building                    ==========
================================================================================
select
 building_id
,nursing_homes_id
,building_name
,create_time
,type
,floor_amount
,floor_room_amount
,room_sacreage
,building_sacreage
,total_sacreage
,location_describe
,use_describe
,inside_room_types
,house_type_id
,orientation
,room_attribute
from db_nursing_home.tb_location_building
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_integral_goods                       ==========
================================================================================
select
id
,integral_goods_id
,integral_goods_name
,use_integral
,create_time
,update_time
,create_user_id
,update_user_id
,status
,org_id
,sub_org_id
from db_pension_organization.tb_integral_goods
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_product                              ==========
================================================================================
select
 product_id       
,dev_sub_key_id         
,device_subtype_id  
,device_type_id     
,developer_id     
,product_version  
,product_icon     
,bind_type        
,bind_type_version
,module_id        
,product_name     
,product_code     
,device_key       
,`mode`           
,brand_id         
,status           
,radiocast_name   
,remark           
,qrcode           
,create_time      
,modify_time      
,bar_code         
,ssid             
,ssid_password    
,program_burning_file     
,functional_schematic_diagram_file as functional_schematic_file
,bill_of_material_file    
,is_auto_config            
,port_number              
,gateway_type             
,third_category_id
from db_device.tb_product
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_device_subtype                       ==========
================================================================================
select
 dev_sub_key_id
,device_subtype_id
,device_type_id
,device_subtype_name
,device_subtype_icon
,status
,remark
,create_time
,modify_time
,is_show
,control_class
from db_device.tb_device_subtype
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_device_type                          ==========
================================================================================
select
 device_type_id
,device_type_name
,device_icon
,status
,remark
,create_time
,modify_time
,is_show
,sub_category_id
,default_device_subtype_id
,all_category_name 
from db_device.tb_device_type
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_trade_channel                        ==========
================================================================================
select
id
,trade_channel_name
,channel_type
,remark
,create_time
,user_id
,org_id
,sub_org_id
from db_nursing_crm.tb_trade_channel
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_trade_channel_type                   ==========
================================================================================
select
 id
,channel_type_name
,remark
,create_time
,user_id
,org_id
,sub_org_id
from db_nursing_crm.tb_trade_channel_type
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_card_product                         ==========
================================================================================
select
 product_id
,create_time
,create_user_id
,org_id
,sub_org_id
,card_id
,batch
,card_fee
,manager_fee
,living_limit
,card_valid
,status
,del_flag
,room_type_option
,discount
,total_deduction
,daily_deduction_limit
,description
,product_num
from db_nursing_crm.tb_card_product
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_card                                 ==========
================================================================================
select
 card_id
,create_time
,create_user_id
,create_user_name
,card_name
,type_option
,type
,org_id
,use_type
,sub_orgs
,inherit_status
,inherit_fee
,transfer_status
,transfer_fee
,return_status
,return_fee
,borrow_status
,borrow_fee
,renewal_status
,renewal_fee
,check_in_status
,max_check_in
,sojourn_status
,max_sojourn
,room_status
,room_count
,discount_status
,discount_des
,health_check_status
,health_check_des
,exclusive_status
,exclusive_des
,priority_residence_status
,priority_residence_des
,travel_status
,travel_des
,age_limit_status
,woman_min_age_limit
,woman_max_age_limit
,man_max_age_limit
,man_min_age_limit
,time_limit_status
,min_time_limit
,max_time_limit
,min_time_unit
,constitution_name
,constitution_url
,description
,del_flag
from db_nursing_crm.tb_card
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_activity_room                        ==========
================================================================================
select
id
,room_name
,status
,location
,nursing_homes_id
,create_time
,start
,end
,available_time_range
,work_id
,update_time
,max_time
from db_nursing_home.tb_activity_room
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_nurse                            ==========
================================================================================
select
 id  
,org_id
,sub_org_id
,orgname as org_name
,suborgname as sub_org_name
,nurse_home_id
,nurse_admin_id
,create_time
,update_time
from db_nursing_home.tb_org_nurse 
where str_to_date(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < str_to_date(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_area                                 ==========
================================================================================
select
 city_id           
,area_id           
,area_name         
,area_english_name 
,weather_code      
from db_acquire_data.tb_area


================================================================================
==========          clife_hr_ods_city                                 ==========
================================================================================
select
 province_id      
,city_id          
,baidu_city_code  
,city_name        
,city_english_name
,longitude        
,latitude         
,is_capital       
from db_acquire_data.tb_city


================================================================================
==========          clife_hr_ods_province                             ==========
================================================================================
select
 partition_id          
,province_id           
,province_name         
,province_english_name 
,longitude             
,latitude              
,baidu_code            
from db_acquire_data.tb_province


================================================================================
==========          clife_hr_ods_partition                            ==========
================================================================================
select
 country_id     
,partition_id   
,partition_name 
from db_acquire_data.tb_partition


================================================================================
==========          clife_hr_ods_country                              ==========
================================================================================
select
 country_id        
,country_name      
,country_en_name   
from db_acquire_data.tb_country


================================================================================
==========          clife_hr_dim_data                                 ==========
================================================================================
null


================================================================================
==========          clife_hr_ods_month_bill                           ==========
================================================================================
select
 id                  
,total_amount       
,bill_time          
,bill_status        
,state              
,org_id             
,sub_org_id         
,create_time        
,update_time        
,old_man_id         
,old_man_check_in_id
from db_pension_organization.tb_hr_month_bill
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_month_bill_child                     ==========
================================================================================
select
 id        
,old_man_id
,UNIX_TIMESTAMP(consumption_time)as consumption_time
,fee_type_id        
,fee_type_name      
,fee_iteam_id       
,fee_iteam_name     
,fee_amount         
,create_time        
,state              
,remark             
,org_id             
,sub_org_id         
,fee_status         
,month_bill_id      
,update_time        
,old_man_check_in_id
,sn                 
,refund_amount      
from db_pension_organization.tb_hr_month_bill_child
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_room_reserve            ==========
================================================================================
select
id
,reserve_time
,activity_room_id
,type
,start_time
,end_time
,nurse_home_id
,create_time
,activity_id
,org_id
,sub_org_id
,reserve_user_id
,reserve_user
,reserve_use
,reserve_user_type
,reserve_source
,reserve_status
,remark
,activity_room_name
,make_time
from db_pension_organization.tb_organization_activity_room_reserve
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_room                    ==========
================================================================================
select
id
,room_name
,room_status
,location
,nursing_homes_id
,create_time
,start
,end
,available_time_range
,work_id
,update_time
,max_time
,org_id
,sub_org_id
,max_num
from db_pension_organization.tb_organization_activity_room
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_template                ==========
================================================================================
select
template_id
,activity_name
,address
,activity_start
,activity_end
,organizer
,activity_desc
,state
,create_time
,update_time
,activity_type
,apply_time
,sign_time
,activity_room_id
,create_user_id
,org_id
,sub_org_id
,source
,activity_img
,apply_max
,app_max
,remind_time
,activity_fee
,activity_object
from db_pension_organization.tb_organization_activity_template
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity                         ==========
================================================================================
select
activity_id
,nurse_home_id
,activity_name
,address
,activity_start
,activity_end
,organizer
,activity_desc
,state
,create_time
,update_time
,activity_type
,apply_time
,sign_time
,activity_room_id
,activity_status
,activity_time
,create_user_id
,org_id
,sub_org_id
,source
,activity_img
,apply_max
,template_id
,app_max
,remind_time
,remark
,activity_fee
,applyed_num
,activity_object
,apply_start_time
,activity_confirm
,activity_cancel
,need_apply
from db_pension_organization.tb_organization_activity
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_participant             ==========
================================================================================
select
activity_id
,old_man_id
,join_time
,sign_time
,sign_status
,apply_type
,apply_flag
,del_flag
,id
,attach_num
,source
,null as payment_status
,null as registration_status
,null as sign_up
,null as pay_cost
,null as activity_fee
,null as old_man_confirm
,remark
from db_pension_organization.tb_organization_activity_participant


================================================================================
==========          clife_hr_ods_account_account                      ==========
================================================================================
select
 account_id
,phone
,email
,password
,source
,reg_time
,create_time
,status
,update_time
,invalid_appId
,create_user_id
from db_account.tb_account
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_monitor_area                         ==========
================================================================================
select
 worker_id
,room_id
,create_time
from db_nursing_home.tb_monitor_area
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_third_worker                         ==========
================================================================================
select
 tb_volunteer_id
,user_id
,work_id
,supplier_id
,tb_volunteer_name
,sex
,card_type
,id_card_no
,link_phone
,update_user_id
,create_user_name
,update_user_name
,clife_user_id
,org_id
,state
,create_time
,update_time
,remark
from db_pension_organization.tb_third_worker
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_worker                               ==========
================================================================================
select
 worker_id
,worker_role_id
,worker_name
,sex
,phone
,user_id
,portrait_url
,nursing_homes_id
,worker_status
,job_title
,is_marketing_manager
from db_nursing_home.tb_worker


================================================================================
==========          clife_hr_ods_old_man_model                        ==========
================================================================================
select
 old_man_id
,module_ids
,create_time
,module_id_sort
from db_nursing_home.tb_old_man_model
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_module                               ==========
================================================================================
select
 id
,module_id
,module_name
,modele_desc
,create_time
,modele_default
from db_nursing_home.tb_module
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_integral_record                      ==========
================================================================================
select
 integral_record_id
,old_man_id
,old_man_name
,integral
,integral_rule_id
,remark
,integral_type
,create_time
,update_time
,create_id
,update_id
,org_id
,sub_org_id
,status
,old_man_check_in_id
from db_pension_organization.tb_integral_record
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_integral_issue_task                  ==========
================================================================================
select
 issue_sign_id
,task_sign_id
,old_man_ids
,issue_time
,deadline_time
,user_id
,org_id
,sub_org_id
,issue_status
,create_time
from db_pension_organization.tb_person_integral_issue_task
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_settle_config                        ==========
================================================================================
select
 settle_config_id
,settle_day
,settle_plan
,org_id
,sub_org_id
,create_time
,update_time
,state
from db_pension_organization.tb_hr_settle_config
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_member_recharge_record               ==========
================================================================================
select
 id
,member_id
,recharge_amount
,pay_type
,payee
,payee_id
,company_account_id
,pay_time
,org_id
,sub_org_id
,state
,receipt_number
,create_time
,update_time
,remark
from db_pension_organization.tb_hr_member_recharge_record
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_admin_sub_org                        ==========
================================================================================
select
     sub_org_id       
    ,org_info_id      
    ,org_type         
    ,parent_org_id    
    ,org_id           
    ,org_name         
    ,org_account      
    ,org_account_id   
    ,org_password     
    ,valid_time       
    ,lose_time        
    ,org_desc         
    ,org_picture      
    ,status           
    ,app_id           
    ,org_code         
    ,org_group_id     
    ,handle_user      
    ,type             
    ,update_time      
    ,create_time      
    ,ubdertake_type   
    ,operation_model  
    ,org_lv           
    ,area             
    ,province         
    ,city             
    ,town             
    ,village          
    ,principal        
    ,link_phone       
    ,address          
    ,sub_org_type     
    ,area_id          
    ,service_province 
    ,service_city     
    ,service_area     
    ,latitude         
    ,longitude
from db_csleep_account.tb_admin_sub_org 
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_nurse_device                         ==========
================================================================================
select
 device_id         
,mac_address       
,device_type_id    
,bind_time         
,auth_user_id      
,device_sub_type_id
,product_id        
,device_num 
from db_nursing_home.tb_nurse_device;


================================================================================
==========          clife_hr_ods_nurse_third_device                   ==========
================================================================================
select
     device_id            
    ,nurse_home_id        
    ,mac                  
    ,type                 
    ,sub_type             
    ,device_num           
    ,device_name          
    ,user_id              
    ,create_time          
from db_nursing_home.tb_nurse_third_device 
where str_to_date(create_time,'%Y-%m-%d') < str_to_date(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_intentional_customer_channel         ==========
================================================================================
select
customer_id
,channel_id
,create_time
from db_nursing_crm.tb_intentional_customer_channel 
where str_to_date(create_time,'%Y-%m-%d') < str_to_date(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_old_man_countermand                  ==========
================================================================================
select
 countermand_id
,old_man_id
,old_man_name
,card_type
,sex
,birthday
,nursing_level
,nursing_level_id
,card_num
,phone
,room_name
,buidlding_id
,floor_id
,room_id
,bed_id
,nursin_homes_id
,reside_time
,countermand_time
,countermand_reason
,file_url
,file_name
,countermand_type
,remark
,countermand_status
,create_user_id
,create_user_name
,update_user_id
,update_user_name
,org_id
,sub_org_id
,status
,create_time
,update_time
,old_man_check_in_id
,agent_id
,agent_name
from db_pension_organization.tb_old_man_countermand
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_leave_reduction                      ==========
================================================================================
select
 leave_reduction_id
,old_leave_id
,status
,discounts_id
,leave_reducation_fee
,fee_name
,nursing_home_id
,org_id
,sub_org_id
,create_time
,update_time
from db_pension_organization.tb_leave_reduction
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_leave_refund                         ==========
================================================================================
select
 refund_id
,old_man_check_in_id
,elder_id
,elder_name
,room
,checkin_type
,old_leave_id
,charging_item_id
,item_number
,refund_type
,name
,day_time
,refund_amount
,create_time
,update_time
,state
,org_id
,sub_org_id
from db_pension_organization.tb_bl_leave_refund
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_leave_discounts                  ==========
================================================================================
select
 leave_discounts_id
,discounts_type
,pay_day
,nursin_homes_id
,sub_org_id
,org_id
,status
,remark
,create_time
,update_time
,create_user_id
,create_user_name
,update_user_id
,update_user_name
from db_pension_organization.tb_organization_finance_leave_discounts
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_old_leave                            ==========
================================================================================
select
 old_leave_id
,old_man_name
,old_man_id
,start_time
,end_time
,leave_time
,accompany_name
,accompany_phone
,remarks
,appendix
,leave_status
,create_time
,update_time
,overtime_status
,return_time
,reality_time
,status
,sub_org_id
,org_id
,create_user_name
,create_user_id
,update_user_id
,update_user_name
,constant_id
,card_type
,card_num
,old_man_check_in_id
,leave_reason
,leave_type
,time_stage
,repatriation_name
,time_stage_return
from db_pension_organization.tb_old_leave
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_evaluation              ==========
================================================================================
select
 evaluation_id
,activity_id
,evaluation_type
,evaluation
,create_time
,old_man_id
,org_id
,sub_org_id
from db_pension_organization.tb_organization_activity_evaluation
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_feedback                ==========
================================================================================
select
 feedback_id
,activity_id
,feedback_type
,remark
,join_sum
,create_time
,user_id
,nurse_home_id
,org_id
,sub_org_id
,feedback_img
from db_pension_organization.tb_organization_activity_feedback
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_nursing_homes_baseinfo               ==========
================================================================================
select
     nursing_homes_id
    ,nursing_homes_name
    ,partition_id
    ,province_id
    ,city_id
    ,area_id
    ,location_detail
    ,account_id
    ,manage_user_id
    ,status
from db_nursing_home.tb_nursing_homes_baseinfo;


================================================================================
==========          clife_hr_ods_member_detail_record                 ==========
================================================================================
select
 fee_detail_record_id
,member_id
,fee_type_id
,fee_type_name
,fee_iteam_id
,fee_iteam_name
,fee_amount
,state
,org_id
,sub_org_id
,create_time
,update_time
from db_pension_organization.tb_hr_member_fee_detail_record
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_org_activity_apply                   ==========
================================================================================
select
 id
,activity_id
,old_man_id
,create_time
,apply_type
,org_id
,sub_org_id
,invite_type
from db_pension_organization.tb_organization_activity_apply
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_increment_evaluate                   ==========
================================================================================
select
 old_man_id
,increment_service_id
,service_name
,service_time
,evaluate_grade
,evaluate_explain
,evaluate_photo
,evaluate_status
,service_status
,evaluate_id
,state
,create_time
,sub_org_id
,old_man_check_in_id
from db_pension_organization.tb_increment_evaluate
where DATE_FORMAT(create_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_membership_apply                     ==========
================================================================================
select 
     apply_id            
    ,create_time         
    ,create_user_id      
    ,org_id              
    ,sub_org_id          
    ,customer_id         
    ,name                
    ,sex_option          
    ,birthday            
    ,phone               
    ,credent_type        
    ,id_card_no          
    ,nationality         
    ,native_province     
    ,native_city         
    ,native_area         
    ,native_address      
    ,live_province       
    ,live_city           
    ,live_area           
    ,live_address
    ,company             
    ,job
    ,hobbies
    ,physical_condition  
    ,disease_history     
    ,membership_id
    ,membership_name     
    ,room_type_option    
    ,checkin_time_option 
    ,checkin_date
    ,checkin_count_option
    ,status
    ,del_flag
from db_nursing_crm.tb_membership_apply
where DATE_FORMAT(create_time,'%Y-%m-%d') <DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_device_category                      ==========
================================================================================
select
 sub_category_id
,category_id
,category_name
,sub_category_name
,status
from db_device.tb_device_category


================================================================================
==========          clife_hr_ods_admin_org                            ==========
================================================================================
select
     org_id           
    ,org_info_id      
    ,org_type         
    ,org_name         
    ,org_account      
    ,org_account_id   
    ,org_password     
    ,valid_time       
    ,lose_time        
    ,org_desc         
    ,org_picture      
    ,status           
    ,app_id           
    ,org_code         
    ,org_group_id     
    ,handle_user      
    ,type             
    ,update_time      
    ,create_time      
    ,principal        
    ,link_phone       
    ,area             
    ,province         
    ,city             
    ,town             
    ,address          
    ,null as latitude         
    ,null as longitude        
from db_csleep_account.tb_admin_org
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_membership_transfer                  ==========
================================================================================
select 
 id                            -- '主键id'
  ,membership_id                 -- '会籍id'
  ,contract_id                   -- '合同id'
  ,pre_transfer_owners           -- '原会籍的权益人，用“,”分割'
  ,pre_contract_id               -- '原合同id'
  ,transfer_date                 -- '转让/继承日期'
  ,state                         -- '会籍转让/继承状态 1-未确认 2-确认转让 3-审批中'
  ,other_amount                  --'其他费用'
  ,other_amount_explain          -- '其他费用说明'
  ,transfer_fee                  --'转让/继承费'
  ,transfer_fee_status           -- '转让/继承费状态 1-未收 2-已收'
  ,audit_user_id                 -- '审批人id'
  ,type                          -- '类型（1-会籍转让 2-会籍继承）'
  ,del_flag                      -- '删除标记（0-否 1-是）'
  ,remark                        --'备注'
  ,user_id                       --'创建账号id'
  ,org_id                        --'集团id'
  ,sub_org_id                    --'机构id'
  ,transfer_nature               --'转让性质(1-赠与，2-转让)'
  ,create_time                   -- '创建时间'
  ,update_time                   -- '更新时间'
  from
  db_nursing_crm.tb_membership_transfer
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_membership_withdraw                  ==========
================================================================================
select 
  id                   --'主键id'
  ,contract_id          --'合同id'
  ,remain_day           --'剩余天数'
  ,apply_date           --'申请日期'
  ,apply_person         --'申请人'
  ,audit_user_id        --'审批人id'
  ,handler_person       --'经手人'
  ,withdraw_reason      --'退会籍原因'
  ,cancle_fee_status    --'是否退会费 1-否 2-是'
  ,del_flag             --'删除标记（0-否 1-是）'
  ,org_id               --'集团id'
  ,sub_org_id           --'机构id'
  ,create_time          --'创建时间'
  ,update_time          --'更新时间'
  ,premium_type         --'退费类型 1：回购 2：身故退费 3:退会籍'
  from 
   db_nursing_crm.tb_membership_withdraw
where DATE_FORMAT(if(update_time is null,create_time,update_time),'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


================================================================================
==========          clife_hr_ods_checkin_pay_record                   ==========
================================================================================
select
 id
,old_man_id
,fee_amount
,fee_item_id
,fee_text
,create_time
,state
,org_id
,sub_org_id
,pay_status
,update_time
,old_man_check_in_id
,reside_type
,receipt_number
,pay_method_id	    
,pay_time
,refund_amount
from db_pension_organization.tb_hr_checkin_pay_record
where DATE_FORMAT(update_time,'%Y-%m-%d') < DATE_FORMAT(now(),'%Y-%m-%d');


