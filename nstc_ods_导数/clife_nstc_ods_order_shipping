select 
     shipping_id                    -- 订单收货信息id
    ,order_id                       -- 订单ID
    ,receiver_name                  -- 收件人
    ,receiver_phone                 -- 电话
    ,receiver_province              -- 省份
    ,receiver_city                  -- 城市
    ,receiver_area                  -- 区县
    ,receiver_town                  -- 街道
    ,receiver_address               -- 地址
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_shipping
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 