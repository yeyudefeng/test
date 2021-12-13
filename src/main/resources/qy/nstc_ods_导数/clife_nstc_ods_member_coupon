select 
     id                             -- ID
    ,member_id                      -- 会员ID
    ,coupon_id                      -- 优惠券ID
    ,`type`                         -- 优惠券类型（1 满减 ）
    ,coupon_name                    -- 优惠券名称
    ,sub_money                      -- 优惠金额
    ,coupon_money                   -- 满减满足金额
    ,order_id                       -- 订单ID
    ,`status`                       -- 优惠券状态（0-未使用，1-已使用，2-已过期，3-已失效）
    ,create_at                      -- 获取时间
    ,order_at                       -- 使用时间
    ,update_at                      -- 更新时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_member_coupon
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 