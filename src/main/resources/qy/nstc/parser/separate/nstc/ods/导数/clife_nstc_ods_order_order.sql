select 
     order_id                       -- 订单id
    ,shop_id                        -- 店铺id
    ,member_id                      -- 会员ID
    ,order_price                    -- 订单总额
    ,payment                        -- 实付金额
    ,order_sn                       -- 订单号
    ,merge_sn                       -- 合单号
    ,order_post                     -- 邮费
    ,coupon_id                      -- 优惠券id
    ,`status`                       -- 订单状态(0-商议订单，1：待支付，2：待发货，3：待收货，4-待评价，5：退货中，6.退货成功，7：订单完成，8：订单取消）
    ,create_at                      -- 下单时间
    ,update_at                      -- 订单更新时间
    ,return_at                      -- 退货时间
    ,pay_at                         -- 支付时间
    ,pay_type                       -- 0-默认未支付1：微信支付，2：支付宝支付 ,3小程序支付 4工行app支付 5 工行pc支付
    ,consign_at                     -- 发货时间
    ,receiving_at                   -- 收货时间
    ,end_at                         -- 交易完成时间
    ,close_at                       -- 交易关闭时间
    ,coment                         -- 是否评价 0：未评价，1：已评价
    ,shipping_type                  -- 物流类型0-公司,1-个人
    ,shipping_name                  -- 物流名称
    ,shipping_code                  -- 物流单号
    ,shipping_reminder              -- 发货提醒0-未提醒,1-已提醒
    ,`read`                         -- 0未读 1已读
    ,buyer_msg                      -- 买家留言
    ,buyer_nick                     -- 买家昵称
    ,`export`                       -- 是否已经导出  0：未导出， 1：已导出
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_order
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 