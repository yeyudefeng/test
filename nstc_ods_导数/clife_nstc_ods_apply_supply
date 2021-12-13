select 
     suppery_Id                    
    ,member_id                     
    ,goodclass                      -- 品类
    ,supper_price                   -- 供货价格
    ,`level`                        -- 商品等级
    ,fromword                       -- 货源地
    ,go                             -- 收获地
    ,image                          -- 图片
    ,conment                        -- 内容
    ,update_at                      -- 更新时间
    ,createby                       -- 创建者
    ,create_at                      -- 创建时间
    ,del_flag                      
    ,title                          -- 标题
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0采购中   1已结束  2被驳回
    ,scannum                        -- 浏览次数
    ,supperNum                      -- 供货数量
from guomiaozhishu.tb_apply_supply
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 
