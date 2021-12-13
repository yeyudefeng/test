insert overwrite table db_dw_nstc.clife_nstc_dim_shop_shop partition(part_date)
select    
     a.shop_id                        -- '店铺信息id' 
    ,a.shop_main                      -- '主营品种' 
    ,a.shop_user_id                   -- '商户ID' 
    ,b.`name`                         -- '商家昵称' 
    ,a.shop_name                      -- '店铺名称' 
    ,a.shop_logo                      -- '店铺图片' 
    ,a.shop_background                -- '背景图' 
    ,a.shop_desc                      -- '店铺简介' 
    ,a.shop_notice                    -- '店铺介绍内容' 
    ,a.shop_type                      -- '认证类型0-个人，1-企业，2-经纪人,10-未认证' 
    ,a.shop_phone                     -- '店铺手机号' 
    ,a.shop_province                  -- '店铺所在省' 
    ,a.shop_city                      -- '店铺所在城市' 
    ,a.shop_area                      -- '店铺所在区域' 
    ,a.shop_town                      -- '乡镇' 
    ,a.shop_address                   -- '经营地址' 
    ,a.shop_star                      -- '信誉值(默认80)' 
    ,a.longitude                      -- '经度' 
    ,a.latitude                       -- '纬度' 
    ,a.create_id                      -- '操作人' 
    ,a.create_at                      -- '创建时间'   
    ,recommend
    ,shop_check
    ,a.part_date                      -- '分区日期'    
from (
    select 
         shop_id                        -- '店铺信息id' 
        ,shop_main                      -- '主营品种' 
        ,shop_user_id                   -- '商户ID' 
        ,shop_name                      -- '店铺名称' 
        ,shop_logo                      -- '店铺图片' 
        ,shop_background                -- '背景图' 
        ,shop_desc                      -- '店铺简介' 
        ,shop_notice                    -- '店铺介绍内容' 
        ,shop_type                      -- '认证类型0-个人，1-企业，2-经纪人,10-未认证' 
        ,shop_phone                     -- '店铺手机号' 
        ,shop_province                  -- '店铺所在省' 
        ,shop_city                      -- '店铺所在城市' 
        ,shop_area                      -- '店铺所在区域' 
        ,shop_town                      -- '乡镇' 
        ,shop_address                   -- '经营地址' 
        ,shop_star                      -- '信誉值(默认80)' 
        ,longitude                      -- '经度' 
        ,latitude                       -- '纬度' 
        ,create_id                      -- '操作人' 
        ,create_at                      -- '创建时间'
        ,part_date                      -- '分区日期'
        ,recommend
        ,shop_check
    from db_ods_nstc.clife_nstc_ods_shop_shop 
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 and disabled = 0
) a 
left join db_ods_nstc.clife_nstc_ods_shop_user b 
on a.part_date = b.part_date and a.shop_user_id = b.shop_user_id and b.del_flag = 0 -- and b.disabled = 0
