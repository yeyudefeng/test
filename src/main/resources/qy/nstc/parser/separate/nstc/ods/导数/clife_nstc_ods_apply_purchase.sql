select 
     purchase_id                    -- 采购id
    ,member_id                      -- 会员id
    ,purchase_num                   -- 采购数量
    ,purchase_price                 -- 价格
    ,purchase_class                 -- 采购品种
    ,purchase_level                 -- (弃用)
    ,address                        -- 收货地址
    ,fromword                       -- 期望货源地
    ,go                             -- 收获地
    ,create_at                     
    ,create_by                      -- 创建者
    ,update_at                     
    ,conent                         -- 内容(选填）
    ,image                          -- 图片地址
    ,del_flag                      
    ,title                          -- 标题
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0发布中 1已结束 2被驳回
    ,scannum                        -- 浏览次数
from guomiaozhishu.tb_apply_purchase
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 