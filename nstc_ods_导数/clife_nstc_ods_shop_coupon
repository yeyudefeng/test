select 
     id                             -- ID
    ,shop_id                        -- 店铺id
    ,coupon_name                    -- 优惠券名称
    ,`type`                         -- 优惠券类型(1-满减)
    ,sub_money                      -- 优惠金额
    ,enough_money                   -- 满减满足金额
    ,total_num                      -- 总数量
    ,send_num                       -- 已发数量
    ,limit_sartAt                   -- 使用期限起
    ,limit_endAt                    -- 使用期限至
    ,limit_number                   -- 允许单人领取数量
    ,has_score                      -- 发送方式（0：余额购买，1：免费发放）
    ,buy_money                      -- 所需余额
    ,`status`                       -- 状态(0可用,1不可用)
    ,create_at                      -- 创建时间
    ,update_at                      -- 更新时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_shop_coupon
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 
