select 
     id                             -- 申诉id
    ,order_id                       -- 订单id
    ,reason_content                 -- 申诉原因
    ,reason_img                     -- 申诉图片
    ,`status`                       -- 处理状态0-未处理,1-已处理
    ,create_at                      -- 申诉时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_appeal
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')