select 
     member_id                      -- 会员id
    ,member_phone                   -- 手机号
    ,member_name                    -- 昵称
    ,member_avator                  -- 头像
    ,`password`                     -- 密码
    ,token                          -- token
    ,`status`                       -- 工行E企付状态0-未提交,1-工行审核中,2-审核通过,3-审核拒绝
    ,intro_user                     -- 推荐人
    ,`level`                        -- 身份0-个人,1-企业,2-经纪人,10-未选择
    ,money                          -- 余额
    ,freeze                         -- 数字金额
    ,aurora_id                      -- 极光推送id
    ,create_at                      -- 注册时间
    ,update_at                      -- 更新时间
    ,unionid                        -- 微信unionid
    ,openid                         -- 微信openId
    ,xcxopenid                      -- 小程序openid
    ,wx_name                        -- 微信昵称
    ,ali_name                       -- 阿里名字
    ,ali_account                    -- 阿里手机号
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)（无用）
    ,del_flag                       -- 删除标记
    ,icbc_status                    -- 工行会员认证0-未认证,1-审核中,2-已认证,3-审核失败,4-提交失败
from guomiaozhishu.tb_member_user
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 