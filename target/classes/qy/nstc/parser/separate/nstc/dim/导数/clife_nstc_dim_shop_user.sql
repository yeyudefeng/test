insert overwrite table db_dw_nstc.clife_nstc_dim_shop_user partition(part_date)
select 
     shop_user_id                   -- 商家账号id
    ,shop_id                        -- 商家id
    ,`level`                        -- 代理等级
    ,phone                          -- 手机号
    ,`name`                         -- 昵称
    ,avator                         -- 头像
    ,intro_user                     -- 推荐人
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)
    ,create_at                      -- 操作时间
    ,update_at                      -- 修改时间
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_nstc.clife_nstc_ods_shop_user
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and del_flag=0