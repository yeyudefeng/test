================================================================================
==========          clife_hr_dim_account                              ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_account partition(part_date)        
select
     a.account_id        
    ,a.old_man_id        
    ,a.membership_id     
    ,a.member_status     
    ,a.account_name      
    ,a.attributes        
    ,a.symbol            
    ,b.nurse_home_id
    ,c.nursing_homes_name
    ,c.area_id           
    ,a.org_id            
    ,b.org_name          
    ,a.sub_org_id        
    ,b.sub_org_name      
    ,a.create_time       
    ,a.update_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from 
(
  select
       account_id        
      ,old_man_id    
      ,null as membership_id     
      ,null as member_status     
      ,account_name      
      ,attributes        
      ,symbol                      
      ,org_id                
      ,sub_org_id             
      ,create_time       
      ,update_time
  from db_ods_hr.clife_hr_ods_account 
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')  
    and state=1
  union all
  select
       id as account_id
      ,null as old_man_id
      ,member_id  as membership_id
      ,member_status
      ,null as account_name
      ,null as attributes
      ,null as symbol
      ,org_id
      ,sub_org_id
      ,create_time
      ,update_time
  from db_ods_hr.clife_hr_ods_member_account
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
    and id<>member_id  
    and state=1
)a
left join db_ods_hr.clife_hr_ods_org_nurse b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_nursing_homes_baseinfo c on b.nurse_home_id = c.nursing_homes_id  and c.part_date = regexp_replace(date_sub(current_date(),1),'-','')    


================================================================================
==========          clife_hr_dim_advisory_customer                    ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_advisory_customer partition(part_date)         
select
 a.advisory_customer_id 
,a.customer_name        
,a.phone_number         
,a.intentional_type     
,a.sex                
,a.reception_date       
,a.sales_consultant_id  
,a.sales_consultant_name
,a.create_time          
,a.update_time          
,a.create_user_id       
,a.org_id               
,b.org_name             
,a.sub_org_id           
,b.sub_org_name         
,a.status
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_advisory_customer a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_assess_report                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_assess_report partition(part_date)         
select
 assess_report_id     
,assess_report_no     
,assess_report_status 
,assess_report_level  
,illness_level        
,old_man_level        
,nursing_level        
,social_support_level 
,create_time          
,update_time          
,org_id               
,sub_org_id           
,state                
,syndrome_number 
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_assess_report a
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')      


================================================================================
==========          clife_hr_dim_card                                 ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_card partition(part_date)         
select
     a.card_id                   
    ,a.create_time               
    ,a.create_user_id            
    ,a.create_user_name          
    ,a.card_name                 
    ,a.type_option               
    ,a.type                      
    ,a.org_id                    
    ,b.org_name                  
    ,a.sub_orgs                  
    ,a.inherit_status            
    ,a.transfer_status           
    ,a.return_status             
    ,a.borrow_status             
    ,a.renewal_status            
    ,a.sojourn_status            
    ,a.discount_status           
    ,a.discount_des              
    ,a.health_check_status       
    ,a.health_check_des          
    ,a.exclusive_status          
    ,a.exclusive_des             
    ,a.priority_residence_status 
    ,a.priority_residence_des    
    ,a.travel_status             
    ,a.travel_des                
    ,a.age_limit_status          
    ,a.woman_min_age_limit       
    ,a.woman_max_age_limit       
    ,a.man_max_age_limit         
    ,a.man_min_age_limit         
    ,a.time_limit_status         
    ,a.min_time_limit            
    ,a.max_time_limit            
    ,a.min_time_unit             
    ,a.constitution_name         
    ,a.constitution_url          
    ,a.description               
    ,case when a.del_flag = 0 then 1 else 0 end as del_flag
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_card a
left join db_ods_hr.clife_hr_ods_admin_org b on a.org_id = b.org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')      


================================================================================
==========          clife_hr_dim_card_product                         ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_card_product partition(part_date)         
select
 a.product_id      
,a.create_time     
,a.create_user_id  
,a.org_id          
,b.org_name        
,a.sub_org_id      
,b.sub_org_name    
,a.card_id         
,a.batch           
,a.status
,case when a.del_flag = 0 then 1 else 0 end as del_flag  
,a.room_type_option
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_card_product a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')      


================================================================================
==========          clife_hr_dim_contract_deposit                     ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_contract_deposit partition(part_date) 
select
     deposit_id     
    ,deposit_num    
    ,intent_user_id 
    ,payee_name     
    ,payee_phone    
    ,membership_id  
    ,membership_name
    ,house_type_id  
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
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date--????????????
from db_ods_hr.clife_hr_ods_contract_deposit a
where part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_contract_sign                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_contract_sign partition(part_date) 
select
     a.sign_id                  
    ,a.contract_num             
    ,a.contract_status          
    ,a.membership_type          
    ,a.membership_name          
    ,a.membership_num           
    ,a.chip_num                 
    ,a.building_id              
    ,a.floor_id                 
    ,a.room_id                  
    ,a.bed_id                   
    ,a.sign_house               
    ,a.originator_type          
    ,a.reason                   
    ,a.is_pay_deposit           
    ,a.pay_state                
    ,a.sign_date                
    ,a.card_valid               
    ,a.house_type               
    ,a.is_divided               
    ,a.membership_effective_time
    ,a.membership_id            
    ,a.product_id               
    ,a.user_id                  
    ,a.sub_org_id               
    ,b.sub_org_name             
    ,a.org_id                   
    ,b.org_name                 
    ,a.cancle_time              
    ,a.is_agency                	
    ,a.agency_id                	
    ,a.payment_type_id          	
    ,a.payment_type_name        	
    ,a.service_fee_id           	
    ,a.is_trust                 	
    ,a.trust_id
    ,case when a.show_status = 0 then 1 
          when a.show_status = 1 then 0  
      else null end as show_status	
    ,case when a.del_flag = 0 then 1 
          when a.del_flag = 1 then 0 
     else null end as del_flag	
    ,a.service_fee_name         	
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_contract_sign a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_customer                             ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_customer partition(part_date) 
select
     a.customer_id         --'????????????id'
    ,a.customer_name       --'??????'
    ,a.customer_type       --????????????  1????????????0?????????
    ,a.sex                 --'????????????id'
    ,a.phone_num           --'????????????'
    ,a.credent_type        --'????????????'
    ,a.id_card_num         --'?????????'
    ,a.birthday            --'??????'
    ,a.reception_date      --'????????????'
    ,a.intentional_type_one--'?????????????????? 1????????????2???????????????3?????????4????????????'
    ,a.intentional_type     --'???????????? 1????????????2???????????????3?????????4????????????'
    ,a.sales_consultant_id           --'????????????id'
    ,a.sales_consultant_name         --'??????????????????'
    ,a.guardian_name                 --'???????????????'
    ,case when a.guardian_sex = 1 then 0
          when a.guardian_sex = 2 then 1
     else a.guardian_sex end as 	guardian_sex	  --'??????????????? 0 ???  1 ??? 2 ??????'
    ,a.relationship                  --'????????????????????????????????? 0-?????? 1-?????? 2-?????? 3-?????? 4-?????? 5-??????'
    ,a.guardian_phone_num            --'?????????????????????'
    ,a.guardian_credent_type         --'?????????????????????'
    ,a.guardian_id_card_num          --'??????????????????'
    ,a.remark                      --'??????'
    ,a.create_time                 --'????????????'
    ,a.create_user_id              --'?????????'
    ,a.create_user_name            --'???????????????'
    ,a.update_time                   --'????????????'
    ,a.sub_org_id                    --'??????id'
    ,b.sub_org_name                    --'????????????'
    ,a.org_id                        --'??????id'
    ,b.org_name                       --'????????????'
    ,a.trade_channel_ids           --'??????id'
    ,a.trade_channel_types         --'????????????'
    ,a.return_visit_remind_time    --'??????????????????'
    ,a.visit_type                  --'??????????????????id'
    ,a.filling_time                --'????????????'
    ,a.questionnaire_id            --'????????????id'
    ,case when del_flag = 0 then 1 
          when del_flag = 1 then 0 
     else null end as del_flag	 --'???????????? 1?????? 0??????'
    ,a.membership_id               --'????????????id'
    ,a.membership_name             --'??????????????????'
    ,a.status                      --'?????? 0????????? 1????????? 2????????? 3????????? | 4?????????'
    ,case when a.phone_visit = 2 then 0 else a.phone_visit end as phone_visit	 
    ,case when a.receive_status = 2 then 0 else a.receive_status end as receive_status	 
    ,a.apartment_type              --'??????????????????'
    ,a.room_type                   --'????????????'
    ,a.vitality_intention          --'????????????????????????'
    ,a.customer_lv                 --'????????????'
    ,a.possible_customer           --'???????????????????????????????????????????????????'
    ,a.cost_type                   --'?????????????????????'
    ,a.age                         --'??????'
    ,a.retire_worker               --'???????????????'
    ,a.hobbies                     --'??????'
    ,a.about_org                   --'????????????'
    ,a.resistance                   --'????????????'
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
select
     customer_id                 --'????????????id'
    ,name as  customer_name      --'??????'
    ,1 as customer_type          --????????????  1????????????0?????????
    ,case when sex_option = 40054 then 0
          when sex_option = 40055 then 1
     else sex_option end as sex           --'????????????id'
    ,phone as phone_num          --'????????????'
    ,credent_type                --'????????????'
    ,id_card_no as id_card_num   --'?????????'
    ,birthday                    --'??????'
    ,null as reception_date      --'????????????'
    ,null as intentional_type_one--'?????????????????? 1????????????2???????????????3?????????4????????????'
    ,intentional_type            --'???????????? 1????????????2???????????????3?????????4????????????'
    ,null as sales_consultant_id           --'????????????id'
    ,null as sales_consultant_name         --'??????????????????'
    ,null as guardian_name                 --'???????????????'
    ,null as guardian_sex                  --'??????????????? 0 ?????? 1 ??? 2 ???'
    ,null as relationship                  --'????????????????????????????????? 0-?????? 1-?????? 2-?????? 3-?????? 4-?????? 5-??????'
    ,null as guardian_phone_num            --'?????????????????????'
    ,null as guardian_credent_type         --'?????????????????????'
    ,null as guardian_id_card_num          --'??????????????????'
    ,remark                      --'??????'
    ,create_time                 --'????????????'
    ,create_user_id              --'?????????'
    ,null as create_user_name            --'???????????????'
    ,null as update_time                   --'????????????'
    ,sub_org_id                    --'??????id'
    ,org_id                        --'??????id'
    ,trade_channel_ids           --'??????id'
    ,trade_channel_types         --'????????????'
    ,return_visit_remind_time    --'??????????????????'
    ,visit_type                  --'??????????????????id'
    ,filling_time                --'????????????'
    ,questionnaire_id            --'????????????id'
    ,del_flag                    --'???????????? 0?????? 1??????'
    ,membership_id               --'????????????id'
    ,membership_name             --'??????????????????'
    ,status                      --'?????? 0????????? 1????????? 2????????? 3????????? | 4?????????'
    ,phone_visit                 --'????????????????????? 1:??? 2??????'
    ,receive_status              --'??????????????????'
    ,apartment_type              --'??????????????????'
    ,room_type                   --'????????????'
    ,vitality_intention          --'????????????????????????'
    ,customer_lv                 --'????????????'
    ,possible_customer           --'???????????????????????????????????????????????????'
    ,cost_type                   --'?????????????????????'
    ,age                         --'??????'
    ,retire_worker               --'???????????????'
    ,hobbies                     --'??????'
    ,about_org                   --'????????????'
    ,resistance                   --'????????????'
from db_ods_hr.clife_hr_ods_intentional_customer
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
union all
select
     intention_customer_id as customer_id         --'??????????????????Id'
    ,customer_name                 --'????????????'
    ,0 as customer_type            --????????????  1????????????0?????????
    ,sex                           --'??????(0:???1???)'
    ,phone_num                     --'????????????'
    ,credent_type                  --'????????????'
    ,id_card_num                   --'?????????'
    ,birthday                      --'??????'
    ,reception_date                --'????????????'
    ,null as intentional_type_one  --'?????????????????? 1????????????2???????????????3?????????4????????????'
    ,intentional_type              --'???????????? 1????????????2???????????????3?????????4????????????'
    ,sales_consultant_id           --'????????????id'
    ,sales_consultant_name         --'??????????????????'
    ,guardian_name                 --'???????????????'
    ,guardian_sex                  --'??????????????? 0 ?????? 1 ??? 2 ???'
    ,relationship                  --'????????????????????????????????? 0-?????? 1-?????? 2-?????? 3-?????? 4-?????? 5-??????'
    ,guardian_phone_num            --'?????????????????????'
    ,guardian_credent_type         --'?????????????????????'
    ,guardian_id_card_num          --'??????????????????'
    ,remark                        --'??????'
    ,create_time                   --'????????????'
    ,create_user_id                --'?????????id'
    ,null as create_user_name      --'???????????????'
    ,update_time                   --'????????????'
    ,sub_org_id                    --'??????id'
    ,org_id                        --'??????id'
    ,null as trade_channel_ids           --'??????id'
    ,null as trade_channel_types         --'????????????'
    ,null as return_visit_remind_time    --'??????????????????'
    ,null as visit_type                  --'??????????????????id'
    ,null as filling_time                --'????????????'
    ,null as questionnaire_id            --'????????????id'
    ,status as del_flag                    --'???????????? 0?????? 1??????'
    ,null as membership_id               --'????????????id'
    ,null as membership_name             --'??????????????????'
    ,null as status                      --'??????'
    ,null as phone_visit                 --'????????????????????? 1:??? 2??????'
    ,null as receive_status              --'??????????????????'
    ,null as apartment_type              --'??????????????????'
    ,null as room_type                   --'????????????'
    ,null as vitality_intention          --'????????????????????????'
    ,null as customer_lv                 --'????????????'
    ,null as possible_customer           --'???????????????????????????????????????????????????'
    ,null as cost_type                   --'?????????????????????'
    ,null as age                         --'??????'
    ,null as retire_worker               --'???????????????'
    ,null as hobbies                     --'??????'
    ,null as about_org                   --'????????????'
    ,null as resistance                   --'????????????'
from db_ods_hr.clife_hr_ods_intention_customer
where part_date=regexp_replace(date_sub(current_date(),1),'-','') 
)a 
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_device                               ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_device partition(part_date)         
select
 a.device_id                 --??????id
,a.product_id                --??????id
,b.device_type_id            --????????????
,a.device_sub_type_id        --????????????
,a.mac_address               --mac??????
,a.device_num                --???????????????
,b.dev_sub_key_id            --'???????????????????????????'
,b.developer_id              --'?????????Id'      
,b.product_version           --'????????????'      
,b.product_icon              --'???????????????'      
,b.bind_type                 --'????????????'      
,b.bind_type_version         --'??????????????????'      
,b.module_id                 --'??????id'      
,b.product_name              --'????????????'      
,b.product_code              --'????????????'      
,b.device_key                --'??????????????????'      
,b.`mode`                    --'??????'      
,b.brand_id                  --'??????ID'      
,b.status                    --'??????'      
,b.radiocast_name            --'???????????????'      
,b.remark                    --'??????'      
,b.qrcode                    --'?????????'          
,b.bar_code                  --'?????????'      
,b.ssid                      --'SSID'
,b.program_burning_file      --'??????????????????'      
,b.functional_schematic_file --'???????????????'      
,b.bill_of_material_file     --'??????????????????'      
,b.is_auto_config            --'????????????????????????????????????'      
,b.port_number               --'???????????????'      
,b.gateway_type              --'???????????????????????????'      
,b.third_category_id         --'?????????????????????id'
,c.all_category_name          --'??????????????????'
,c.default_device_subtype_id  --'????????????4.0?????????????????????id????????????????????????id???????????????'
,c.device_type_name           --'??????????????????'
,c.device_icon                --'????????????'
,c.sub_category_id            --'?????????????????????id'
,c.is_show                    --'????????????'
,e.control_class              --'??????????????????'
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from
(
  select
       device_id      --'??????id',
      ,mac_address    --'mac??????',
      ,product_id     --'??????id',
      ,device_type_id --'????????????',
      ,device_sub_type_id  --'????????????'
      ,device_num          --'???????????????'
  from db_ods_hr.clife_hr_ods_nurse_device
  where part_date = regexp_replace(date_sub(current_date(),1),'-','')
  union all
  select
       device_id                --'??????id',
      ,mac as mac_address       --'mac??????',
      ,cast(type as bigint) as product_id       --'??????id',
      ,null as device_type_id   --'????????????',
      ,sub_type as device_sub_type_id  --'????????????'
      ,device_num                      --'????????????'
  from db_ods_hr.clife_hr_ods_nurse_third_device
  where part_date = regexp_replace(date_sub(current_date(),1),'-','')
)a
left join db_ods_hr.clife_hr_ods_product b on a.product_id=b.product_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_device_type c on b.device_type_id=c.device_type_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_device_category d on d.sub_category_id=c.sub_category_id and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_device_subtype e on b.dev_sub_key_id=e.dev_sub_key_id and e.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_geographic_info                      ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_geographic_info partition(part_date) 
select 
     a.area_id	                           --???????????????id
    ,a.area_name	                       --?????????????????????
    ,a.city_id	                           --??????id
    ,b.baidu_city_code	                   --??????????????????
    ,b.city_name	                       --????????????
    ,b.city_english_name	               --??????????????????
    ,b.longitude	as  city_longitude     --????????????
    ,b.latitude	as  city_latitude          --????????????
    ,b.is_capital                          --?????????????????????
    ,b.province_id	                       --??????id
    ,c.province_name	                   --????????????
    ,c.province_english_name               --???????????????
    ,c.longitude	as province_longitude  --????????????
    ,c.latitude	as province_latitude       --????????????
    ,c.baidu_code                          --??????????????????
    ,d.partition_id                        --???????????????id
    ,d.partition_name                      --?????????????????????
    ,d.country_id                          --??????id
    ,e.country_name                        --????????????
    ,e.country_en_name                     --???????????????
    ,regexp_replace(date_sub(from_unixtime(unix_timestamp() ,'yyyy-MM-dd'),1),'-','')
from db_ods_hr.clife_hr_ods_area a
left join db_ods_hr.clife_hr_ods_city  b on a.city_id = b.city_id
left join db_ods_hr.clife_hr_ods_province c on  b.province_id = c.province_id
left join db_ods_hr.clife_hr_ods_partition d on  c.partition_id = d.partition_id
left join db_ods_hr.clife_hr_ods_country e on  d.country_id = e.country_id


================================================================================
==========          clife_hr_dim_integral_goods                       ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_integral_goods partition(part_date) 
select
     a.id                  
    ,a.integral_goods_id  
    ,a.integral_goods_name
    ,a.create_time        
    ,a.update_time        
    ,a.create_user_id     
    ,a.update_user_id     
    ,a.status             
    ,a.org_id             
    ,a.sub_org_id         
    ,b.org_name           
    ,b.sub_org_name        
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_integral_goods a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_membership                           ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_membership partition(part_date) 
select
     a.id               
    ,a.contract_id      
    ,a.membership_status
    ,a.return_status    
    ,a.user_id          
    ,a.sub_org_id       
    ,b.sub_org_name     
    ,a.org_id           
    ,b.org_name
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_membership a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_location_room                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_location_room partition(part_date) 
select
 a.room_id
,a.floor_id
,b.floor_name
,a.nursing_homes_id
,a.room_type
,a.house_type_id
,a.house_type_name
,a.orientation
,a.orientation_name
,a.room_attribute
,case when a.sales_status = 0 then 1
      when a.sales_status = 1 then 0
 else null end as sales_status
,a.room_name
,a.inside_room_types
,c.building_id
,c.building_name
,c.type
,c.floor_amount
,c.floor_room_amount
,c.location_describe
,c.use_describe
,d.bed_amount
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_location_room a
left join db_ods_hr.clife_hr_ods_location_floor b on a.floor_id = b.floor_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_location_building c on b.building_id = c.building_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join 
(
  select room_id,count(bed_id) as bed_amount
  from db_ods_hr.clife_hr_ods_location_bed  
  where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  group by room_id
)d on a.room_id=d.room_id
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_membership_owner                     ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_membership_owner partition(part_date)         
select
     a.id
    ,a.name
    ,case when a.sex = 1 then 0 
          when a.sex = 2 then 1 
     else a.sex end as sex
    ,a.birthday
    ,a.phone                  
    ,a.id_card
    ,a.card_type
    ,a.age
    ,a.census_register
    ,a.census_province_id
    ,a.census_province_name   
    ,a.census_city_id
    ,a.census_city_name
    ,a.census_area_id
    ,a.census_area_name       
    ,a.residence
    ,a.residence_province_id
    ,a.residence_province_name
    ,a.residence_city_id
    ,a.residence_city_name
    ,a.residence_area_id
    ,a.residence_area_name    
    ,a.unit
    ,a.title
    ,a.hobbies
    ,a.physical_condition
    ,a.disease_history
    ,a.`type`
    ,a.user_id                
    ,a.org_id
    ,b.org_name
    ,a.sub_org_id
    ,b.sub_org_name
    ,case when a.status = 0 then 1
          when a.status = 1 then 0
     else null end as status		  
    ,a.audit_user_id
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_membership_owner a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_nursing_home                         ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_nursing_home partition(part_date) 
select 
     a.nursing_homes_id        --????????????id
    ,a.nursing_homes_name
    ,f.sub_org_id
    ,f.org_id
    ,a.partition_id
    ,a.province_id
    ,a.city_id
    ,a.area_id
    ,a.location_detail
    ,a.account_id
    ,a.status
    ,f.org_name
    ,f.sub_org_name
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date--????????????
from db_ods_hr.clife_hr_ods_nursing_homes_baseinfo a  
left join db_ods_hr.clife_hr_ods_org_nurse f on a.nursing_homes_id = f.nurse_home_id and f.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_old_man_contract                     ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_old_man_contract partition(part_date) 
select
     a.contract_id       
    ,a.pre_contract_id    
    ,b.old_man_name      
    ,b.old_man_id        
    ,b.sex               
    ,b.phone      
    ,b.card_type      
    ,b.card_num         
    ,a.state            
    ,b.bed_id            
    ,b.room_attribute  
    ,b.room_name  
    ,b.room_id           
    ,b.floor_id          
    ,b.building_id       
    ,a.org_id            
    ,a.sub_org_id        
    ,f.nurse_home_id  
    ,b.create_user_id    
    ,b.create_user_name  
    ,b.create_time       
    ,b.update_user_id    
    ,b.update_user_name  
    ,b.update_time       
    ,c.org_name       
    ,c.sub_org_name      
    ,d.nursing_homes_name 	
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_old_man_contract a
left join db_ods_hr.clife_hr_ods_check_in b on a.old_man_check_in_id = b.old_man_check_in_id and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_admin_sub_org c on a.sub_org_id = c.sub_org_id  and c.part_date = regexp_replace(date_sub(current_date(),1),'-','')   
left join db_ods_hr.clife_hr_ods_org_nurse f on a.sub_org_id = f.sub_org_id and f.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_nursing_homes_baseinfo d on f.nurse_home_id = d.nursing_homes_id  and d.part_date=regexp_replace(date_sub(current_date(),1),'-','')       
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_trade_channel                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_trade_channel partition(part_date)         
select
 a.id
,a.trade_channel_name
,a.channel_type
,c.channel_type_name
,a.remark
,a.create_time
,a.user_id
,a.org_id
,b.org_name
,a.sub_org_id
,b.sub_org_name
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_trade_channel a
left join db_dw_hr.clife_hr_dim_admin_sub_org b on a.sub_org_id = b.sub_org_id  and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')
left join db_ods_hr.clife_hr_ods_trade_channel_type c on a.channel_type=c.id and c.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_worker_new                           ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_worker_new partition(part_date) 
select
 a.worker_id       
,a.worker_role_id  
,a.worker_name     
,a.credent_type    
,a.credent_number  
,a.sex
,a.birthday        
,if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr(a.birthday,5,7)))>=0,
(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)),
(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)-1)) as age             
,a.natives_province
,a.natives_city    
,a.native_address  
,a.worker_type     
,a.sub_org_id      
,c.sub_org_name    
,a.org_id          
,c.org_name        
,a.phone           
,a.user_id
,a.portrait_url    
,a.nurse_home_id   
,a.worker_status   
,a.personnel_type  
,a.occupation_type 
,a.position        
,a.department_name 
,a.department_id   
,a.live_address
,a.nurse_worker_id
,regexp_replace(date_sub(current_date(),1),'-','') as part_date--????????????
from db_ods_hr.clife_hr_ods_worker_new a
left join db_dw_hr.clife_hr_dim_admin_sub_org c on a.sub_org_id = c.sub_org_id  and c.part_date = regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_old_man                              ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_old_man partition(part_date) 
select 
 a.old_man_id           
,a.old_man_name         
,a.nursin_homes_id      
,a.id_card_no           
,a.sex             
,a.birthday             
,a.lunar_birthday       
,a.solar_birthday       
,a.illness_case_ids     
,a.location_id          
,c.live_address         
,c.live_city            
,a.portrait_url         
,a.phone                
,a.contact_information  
,c.nation               
,c.social_security_card 
,c.card_place           
,c.card_hospital        
,c.education_degree     
,c.religion             
,c.marital_status       
,c.living_status        
,c.medical_costs        
,c.finance_source       
,c.dementia             
,c.mental_disease       
,c.native_address       
,c.native_place         
,c.guardian             
,c.guardian_contactInfo 
,c.guardian_phone       
,c.guardian_relationship
,c.guardian_email       
,c.ability_level        
,c.politic_countenance  
,c.blood_type           
,c.is_local_household_registration
,c.retirement_career     
,c.emergency_name        
,c.emergency_relationship
,c.emergency_contactInfo 
,concat_ws('-',d.province_name,d.city_name,d.area_name) as birthplace
,a.old_man_startus        
,a.admission_date        
,a.checkin_date          
,a.nurse_point_ids       
,a.nurse_man_id          
,a.need_nurse            
,a.location_id as room_id
,a.bed_id                      
,a.user_id               
,a.center_id             
,a.nursing_level_id      
,a.nursing_level         
,a.service_scheme_id     
,a.service_scheme_name  
,if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr(a.birthday,5,7)))>=0,
	(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)),
	(substr(CURRENT_DATE,0,4) - substr(a.birthday,0,4)-1)) as age  
,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_old_man a
left join db_ods_hr.clife_hr_ods_old_man_extend c on a.old_man_id=c.old_man_id and c.part_date=regexp_replace(date_sub(current_date(),1),'-','')
left join db_dw_hr.clife_hr_dim_geographic_info d on c.area = d.area_id
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_activity_room                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_activity_room partition(part_date) 
select
     a.id                 
    ,a.room_name         
    ,a.room_status
    ,a.location          
    ,a.nursing_homes_id  
    ,b.nursing_homes_name
    ,b.org_id            
    ,b.org_name          
    ,b.sub_org_id        
    ,b.sub_org_name      
    ,a.create_time       
    ,a.`start`             
    ,a.`end`               
    ,a.work_id           
    ,a.update_time
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_org_activity_room a
left join  db_dw_hr.clife_hr_dim_nursing_home b on a.nursing_homes_id = b.nursing_homes_id and b.part_date = regexp_replace(date_sub(current_date(),1),'-','')  
where a.part_date = regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_admin_sub_org                        ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_admin_sub_org partition(part_date) 
select
     a.sub_org_id       
    ,a.org_info_id      
    ,a.org_type         
    ,a.parent_org_id    
    ,a.org_id           
    ,a.org_name as  sub_org_name       
    ,a.org_account      
    ,a.org_account_id   
    ,a.org_password     
    ,a.valid_time       
    ,a.lose_time        
    ,a.org_desc         
    ,a.org_picture      
    ,a.status           
    ,a.app_id           
    ,a.org_code         
    ,a.org_group_id     
    ,a.handle_user      
    ,a.type             
    ,a.update_time      
    ,a.create_time      
    ,a.ubdertake_type   
    ,a.operation_model  
    ,a.org_lv           
    ,a.area             
    ,a.province         
    ,a.city             
    ,a.town             
    ,a.village          
    ,a.principal        
    ,a.link_phone       
    ,a.address          
    ,a.sub_org_type     
    ,a.area_id          
    ,a.service_province 
    ,a.service_city     
    ,a.service_area     
    ,a.latitude         
    ,a.longitude
    ,b.org_name
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_hr.clife_hr_ods_admin_sub_org a
left join db_ods_hr.clife_hr_ods_admin_org b on a.org_id=b.org_id  and b.part_date=regexp_replace(date_sub(current_date(),1),'-','')
where a.part_date=regexp_replace(date_sub(current_date(),1),'-','')


================================================================================
==========          clife_hr_dim_admin_org                            ==========
================================================================================
insert overwrite table db_dw_hr.clife_hr_dim_admin_org partition(part_date) 
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
    ,latitude         
    ,longitude
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date	
from db_ods_hr.clife_hr_ods_admin_org a
where part_date=regexp_replace(date_sub(current_date(),1),'-','')


