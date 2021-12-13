select 
     rent_id                        -- 出租拉货id
    ,title                          -- 标题
    ,price                          -- 价格
    ,area                           -- 货车类型
    ,stcity                         -- 起运城市
    ,staddress                      -- 起运地址
    ,encity                         -- 目的城市
    ,address                        -- 目的地范围
    ,conent                         -- 备注内容
    ,image                          -- 图片
    ,create_at                      -- 发布时间
    ,create_by                      -- 创造者
    ,member_id                      -- 会员id
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0采购中   1已结束  2被驳回
    ,scannum                        -- 浏览量
    ,longitude                      -- 经度
    ,latitude                       -- 纬度
    ,localNo                        -- 库位
    ,update_at                      -- 更新时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_storage_rent
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 
