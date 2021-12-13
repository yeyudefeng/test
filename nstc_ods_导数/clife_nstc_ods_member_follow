select 
     follow_id                     
    ,goods_id                       -- 商品id
    ,member_id                      -- 会员id
    ,create_at                      -- 创建时间
from guomiaozhishu.tb_member_follow
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')