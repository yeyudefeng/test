select 
     id                             -- 店铺访问记录
    ,member_id                      -- 会员id
    ,shop_id                        -- 店铺id
    ,`type`                         -- 新老访客0-新访客,1-老访客
    ,create_at                      -- 访问时间
    ,create_day                     -- 访问日期
from guomiaozhishu.tb_record_shop_member
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')