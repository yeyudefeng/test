select 
     id                             -- 访问记录id
    ,member_id                      -- 会员id
    ,goods_id                       -- 商品id
    ,shop_id                        -- 店铺id
    ,`type`                         -- 新老访客数0-新访客,1-老访客
    ,create_at                      -- 访问时间
    ,create_day                     -- 访问日期
from guomiaozhishu.tb_record_goods_member
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')