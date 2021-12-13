insert overwrite table db_dw_nstc.clife_nstc_dim_member_address partition(part_date)
select   
     address_id                     -- '收货地址id' 
    ,member_id                      -- '会员ID' 
    ,province                       -- '所在省' 
    ,city                           -- '所在市' 
    ,area                           -- '所在县区' 
    ,town                           -- '所在街道' 
    ,address                        -- '详细地址' 
    ,post_code                      -- '邮政编码' 
    ,full_name                      -- '收货人姓名' 
    ,phone                          -- '收货人电话' 
    ,default_value                  -- '是否默认0-非默认,-1默认' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_member_address
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 