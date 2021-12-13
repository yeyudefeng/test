select 
     id                            
    ,shop_id                        -- 店铺id
    ,member_id                      -- 会员id
    ,create_at                      -- 关注时间
from guomiaozhishu.tb_member_shop
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')