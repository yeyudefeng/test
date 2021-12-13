select 
     shop_user_id                   -- 商家账号id
    ,shop_id                        -- 商家id
    ,`level`                        -- 代理等级
    ,phone                          -- 手机号
    ,`password`                     -- 密码
    ,token                          -- token
    ,`name`                         -- 昵称
    ,avator                         -- 头像
    ,intro_user                     -- 推荐人
    ,aurora_id                      -- 极光推送id
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)
    ,create_at                      -- 操作时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_shop_user
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 