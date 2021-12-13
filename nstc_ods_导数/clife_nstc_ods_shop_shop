select 
     shop_id                        -- 店铺信息id
    ,shop_main                      -- 主营品种
    ,shop_user_id                   -- 商户ID
    ,shop_name                      -- 店铺名称
    ,shop_logo                      -- 店铺图片
    ,shop_background                -- 背景图
    ,shop_desc                      -- 店铺简介
    ,shop_notice                    -- 店铺介绍内容
    ,shop_type                      -- 认证类型0-个人，1-企业，2-经纪人,10-未认证
    ,shop_phone                     -- 店铺手机号
    ,shop_province                  -- 店铺所在省
    ,shop_city                      -- 店铺所在城市
    ,shop_area                      -- 店铺所在区域
    ,shop_town                      -- 乡镇
    ,shop_address                   -- 经营地址
    ,shop_sale                      -- 总销量
    ,shop_star                      -- 信誉值(默认80)
    ,longitude                      -- 经度
    ,latitude                       -- 纬度
    ,recommend                      -- 首页展示0-不展示,1-展示
    ,hits                           -- 浏览量
    ,shop_check                     -- 审核0-审核中,1-成功,2-失败
    ,money                          -- 账户余额
    ,freeze                         -- 冻结金额
    ,all_money                      -- 总成交额
    ,bank_card                      -- 银行卡号
    ,bank_user_name                 -- 持卡人姓名
    ,bank_name                      -- 银行名字
    ,bank_address                   -- 开户行地址
    ,intro_user                     -- 推荐人
    ,ali_number                     -- 支付宝账号
    ,ali_name                       -- 支付宝名字
    ,times                          -- 发货时间
    ,location                       -- 排序
    ,create_id                      -- 操作人
    ,create_at                      -- 创建时间
    ,disabled                       -- 是否禁用(0正常)
    ,del_flag                       -- 删除标记
    ,batchId                       
from guomiaozhishu.tb_shop_shop
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')