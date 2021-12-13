
insert overwrite table db_dw_nstc.clife_nstc_dim_shop_coupon partition(part_date)
select
     id                             -- 'ID' 
    ,shop_id                        -- '店铺id' 
    ,coupon_name                    -- '优惠券名称' 
    ,`type`                         -- '优惠券类型(1-满减)' 
    ,sub_money                      -- '优惠金额' 
    ,enough_money                   -- '满减满足金额' 
    ,total_num                      -- '总数量' 
    ,limit_sartAt                   -- '使用期限起' 
    ,limit_endAt                    -- '使用期限至' 
    ,limit_number                   -- '允许单人领取数量' 
    ,has_score                      -- '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      -- '所需余额' 
    ,`status`                       -- '状态(0可用,1不可用)' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '更新时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_shop_coupon
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
