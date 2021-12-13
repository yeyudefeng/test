select 
     detail_id                      -- 订单详情id
    ,order_id                       -- 订单ID
    ,goods_id                       -- 商品ID
    ,product_id                     -- 产品规格ID
    ,order_sn                       -- 子订单号
    ,total                          -- 购买数量
    ,coment                         -- 0未评价  1已评价
    ,price                          -- 单价
    ,all_price                      -- 总价
    ,create_at                      -- 创建时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_detail
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')