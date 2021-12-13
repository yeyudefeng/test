select 
     lease_id                       -- 用车id
    ,member_id                      -- 会员id
    ,title                          -- 标题
    ,price                          -- 价格
    ,local_no                       -- 冷库库位
    ,stcity                         -- 起运城市
    ,address                        -- 起运地详细地址
    ,encity                         -- 目的城市
    ,area                           -- 目的地地址
    ,content                        -- 备注
    ,image                          -- 图片
    ,create_at                      -- 发布时间
    ,create_by                      -- 创建人
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0采购中   1已结束  2被驳回
    ,scannum                        -- 浏览次数
    ,longitude                      -- 经度
    ,latitude                       -- 纬度
    ,update_at                      -- 更新时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_storage_lease
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 