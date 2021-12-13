select 
     back_id                        -- 退换货id
    ,order_id                       -- 订单ID
    ,price                          -- 退款金额
    ,express_name                   -- 物流名/司机电话
    ,express_code                   -- 快物流单号/车牌号
    ,`type`                         -- 类型0-公司物流,1-个人找车
    ,`status`                       -- 状态0-审核中,1-同意,2-拒绝
    ,order_status                   -- 订单原状态
    ,reason                         -- 退货原因
    ,back_type                      -- 0-未发货退款、1-已发货未收到货退款、2-收到货仅退款、3-收到货退货退款
    ,reason_f                       -- 驳回原因
    ,create_at                      -- 创建时间
    ,update_at                      -- 更新时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_back
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 