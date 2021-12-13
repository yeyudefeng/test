select 
     address_id                     -- 收货地址id
    ,member_id                      -- 会员ID
    ,province                       -- 所在省
    ,city                           -- 所在市
    ,area                           -- 所在县区
    ,town                           -- 所在街道
    ,address                        -- 详细地址
    ,post_code                      -- 邮政编码
    ,full_name                      -- 收货人姓名
    ,phone                          -- 收货人电话
    ,default_value                  -- 是否默认0-非默认,-1默认
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_member_address
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 
