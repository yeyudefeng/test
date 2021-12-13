select 
     comment_id                     -- 评论表id
    ,detail_id                      -- 订单详情表id
    ,goods_id                       -- 商品id
    ,product_id                     -- 产品id
    ,member_id                      -- 评论人
    ,star                           -- 评价星级 1-5,默认5星
    ,content                        -- 评价内容
    ,comment_img                    -- 评论图片
    ,disply                         -- 删除标记 0：未删除，1：删除
    ,create_at                      -- 创建时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_coment
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')