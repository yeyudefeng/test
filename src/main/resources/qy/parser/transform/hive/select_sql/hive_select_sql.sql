====================================================================================================
==========          guomiaozhishu.tb_township_area                                        ==========
==========          db_ods_nstc.clife_nstc_ods_township_area                              ==========
====================================================================================================
select 
     id                             -- id
    ,name_prov                      -- 省
    ,code_prov                      -- 省code
    ,name_city                      -- 市
    ,code_city                      -- 市code
    ,name_coun                      -- 区
    ,code_coun                      -- 区code
    ,name_town                      -- 镇
    ,code_town                      -- 镇code
from guomiaozhishu.tb_township_area


====================================================================================================
==========          guomiaozhishu.tb_shop_user                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_user                                  ==========
====================================================================================================
select 
     shop_user_id                   -- 商家账号id
    ,shop_id                        -- 商家id
    ,`level`                        -- 代理等级
    ,phone                          -- 手机号
    ,`password`                     -- 密码
    ,token                          -- token
    ,`name`                         -- 昵称
    ,avator                         -- 头像
    ,intro_user                     -- 推荐人
    ,aurora_id                      -- 极光推送id
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)
    ,create_at                      -- 操作时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_shop_user
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_shop_shop                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_shop                                  ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_shop_coupon                                          ==========
==========          db_ods_nstc.clife_nstc_ods_shop_coupon                                ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_record_shop_member                                   ==========
==========          db_ods_nstc.clife_nstc_ods_record_shop_member                         ==========
====================================================================================================
select 
     id                             -- 店铺访问记录
    ,member_id                      -- 会员id
    ,shop_id                        -- 店铺id
    ,`type`                         -- 新老访客0-新访客,1-老访客
    ,create_at                      -- 访问时间
    ,create_day                     -- 访问日期
from guomiaozhishu.tb_record_shop_member
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_record_goods_member                                  ==========
==========          db_ods_nstc.clife_nstc_ods_record_goods_member                        ==========
====================================================================================================
select 
     id                             -- 访问记录id
    ,member_id                      -- 会员id
    ,goods_id                       -- 商品id
    ,shop_id                        -- 店铺id
    ,`type`                         -- 新老访客数0-新访客,1-老访客
    ,create_at                      -- 访问时间
    ,create_day                     -- 访问日期
from guomiaozhishu.tb_record_goods_member
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_qa_question                                          ==========
==========          db_ods_nstc.clife_nstc_ods_qa_question                                ==========
====================================================================================================
select 
     question_id                    -- 问题id
    ,member_id                      -- 会员id
    ,question_content               -- 问题描述
    ,question_img                   -- 图片链接
    ,`status`                       -- 显示状态0-显示,1-隐藏
    ,like_num                       -- 点赞数
    ,create_at                      -- 发布时间
    ,del_flag                       -- 删除标记0-正常
from guomiaozhishu.tb_qa_question
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_qa_answer                                            ==========
==========          db_ods_nstc.clife_nstc_ods_qa_answer                                  ==========
====================================================================================================
select 
     answer_id                      -- 答案id
    ,question_id                    -- 问题id
    ,member_id                      -- 回答人id
    ,answer_content                 -- 回答内容
    ,`status`                       -- 显示状态0-显示,1-隐藏
    ,like_num                       -- 回复点赞数
    ,create_at                      -- 回答时间
    ,del_flag                       -- 删除标记0-正常
from guomiaozhishu.tb_qa_answer
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_order_shipping                                       ==========
==========          db_ods_nstc.clife_nstc_ods_order_shipping                             ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_order_order                                          ==========
==========          db_ods_nstc.clife_nstc_ods_order_order                                ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_order_detail                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_detail                               ==========
====================================================================================================
select 
     detail_id                      -- 订单详情id
    ,order_id                       -- 订单ID
    ,goods_id                       -- 商品ID
    ,product_id                     -- 产品规格ID
    ,order_sn                       -- 子订单号
    ,total                          -- 购买数量
    ,coment                         -- 0未评价  1已评价
    ,price                          -- 单价
    ,all_price                      -- 总价
    ,create_at                      -- 创建时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_order_detail
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_order_coment                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_coment                               ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_order_back                                           ==========
==========          db_ods_nstc.clife_nstc_ods_order_back                                 ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_order_appeal                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_appeal                               ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_member_user                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_user                                ==========
====================================================================================================
select 
     member_id                      -- 会员id
    ,member_phone                   -- 手机号
    ,member_name                    -- 昵称
    ,member_avator                  -- 头像
    ,`password`                     -- 密码
    ,token                          -- token
    ,`status`                       -- 工行E企付状态0-未提交,1-工行审核中,2-审核通过,3-审核拒绝
    ,intro_user                     -- 推荐人
    ,`level`                        -- 身份0-个人,1-企业,2-经纪人,10-未选择
    ,money                          -- 余额
    ,freeze                         -- 数字金额
    ,aurora_id                      -- 极光推送id
    ,create_at                      -- 注册时间
    ,update_at                      -- 更新时间
    ,unionid                        -- 微信unionid
    ,openid                         -- 微信openId
    ,xcxopenid                      -- 小程序openid
    ,wx_name                        -- 微信昵称
    ,ali_name                       -- 阿里名字
    ,ali_account                    -- 阿里手机号
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)（无用）
    ,del_flag                       -- 删除标记
    ,icbc_status                    -- 工行会员认证0-未认证,1-审核中,2-已认证,3-审核失败,4-提交失败
from guomiaozhishu.tb_member_user
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_member_shop                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_shop                                ==========
====================================================================================================
select 
     id                            
    ,shop_id                        -- 店铺id
    ,member_id                      -- 会员id
    ,create_at                      -- 关注时间
from guomiaozhishu.tb_member_shop
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_member_question                                      ==========
==========          db_ods_nstc.clife_nstc_ods_member_question                            ==========
====================================================================================================
select 
     member_id                      -- 会员id
    ,question_id                    -- 问题id
from guomiaozhishu.tb_member_question


====================================================================================================
==========          guomiaozhishu.tb_member_follow                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_follow                              ==========
====================================================================================================
select 
     follow_id                     
    ,goods_id                       -- 商品id
    ,member_id                      -- 会员id
    ,create_at                      -- 创建时间
from guomiaozhishu.tb_member_follow
where date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')


====================================================================================================
==========          guomiaozhishu.tb_member_coupon                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_coupon                              ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_member_answer                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_answer                              ==========
====================================================================================================
select 
     member_id                      -- 会员id
    ,answer_id                      -- 回复id
from guomiaozhishu.tb_member_answer


====================================================================================================
==========          guomiaozhishu.tb_member_address                                       ==========
==========          db_ods_nstc.clife_nstc_ods_member_address                             ==========
====================================================================================================
select 
     address_id                     -- 收货地址id
    ,member_id                      -- 会员ID
    ,province                       -- 所在省
    ,city                           -- 所在市
    ,area                           -- 所在县区
    ,town                           -- 所在街道
    ,address                        -- 详细地址
    ,post_code                      -- 邮政编码
    ,full_name                      -- 收货人姓名
    ,phone                          -- 收货人电话
    ,default_value                  -- 是否默认0-非默认,-1默认
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_member_address
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_goods_product                                        ==========
==========          db_ods_nstc.clife_nstc_ods_goods_product                              ==========
====================================================================================================
select 
     product_id                     -- 产品id
    ,batch_id                       -- 批次id
    ,sku                            -- SKU
    ,goods_id                       -- 商品ID
    ,product_name                   -- 货品名
    ,product_img                    -- 产品图片
    ,product_spec                   -- 货品规格
    ,product_price                  -- 价格
    ,product_post                   -- 期货购买价
    ,product_weight                 -- 一包的重量
    ,product_stock                  -- 库存
    ,product_buy_min                -- 期货卖出一包的重量
    ,product_buy_max                -- 期货卖出库存
    ,product_unit                   -- 计量单位
    ,product_loading                -- 是否下架(0审核1-上架2下架,3-拒绝,5-卖出)
    ,product_default                -- 是否默认(1是默认)
    ,loading_at                     -- 上架时间
    ,unloading__at                  -- 下架时间
    ,product_sale_num               -- 销售量
    ,product_location               -- 排序字段
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
    ,store_name                     -- 仓库名
from guomiaozhishu.tb_goods_product
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_goods_goods                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_goods                                ==========
====================================================================================================
select 
     goods_id                       -- 商品id
    ,shop_id                        -- 店铺id
    ,goods_name                     -- 商品名称
    ,goods_title                    -- 商品标题
    ,class_id                       -- 商品分类
    ,type_id                        -- 商品类型
    ,goods_img                      -- 商品主图
    ,goods_note                     -- 0-农资商品,1-苗木商品
    ,goods_prop                     -- 商品等级
    ,goods_spec                     -- 规格详情
    ,goods_param                    -- 参数详情
    ,has_spec                       -- 启用规格
    ,goods_unit                     -- 计量单位
    ,goods_loading                  -- 是否下架(0-待上架，1-上架,2下架，3-待审核,4-拒绝5-卖出)
    ,examine                        -- 审核状态0-未审核,1-审核中,2-已通过,3-拒绝
    ,goods_reject                   -- 驳回原因
    ,goods_view_num                 -- 浏览量
    ,goods_comment_num              -- 评论量
    ,goods_sale_num                 -- 销售量
    ,goods_location                 -- 排序字段
    ,create_id                      -- 创建用户id
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记0-显示,1-删除
    ,recom_sy                       -- 0-优品优选,1-乡村振兴
    ,recommend                      -- 商品推荐0-不推荐,1-推荐
    ,recom_xq                       -- 邮费,0-包邮,1-不包邮
    ,recom_gwc                      -- 挂牌0-普通,1-挂牌
    ,brand_id                       -- 商品品牌
    ,unloading_at                   -- 下架时间
    ,loading_at                     -- 上架时间
from guomiaozhishu.tb_goods_goods
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_goods_class                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_class                                ==========
====================================================================================================
select 
     class_id                       -- 商品分类id
    ,parent_id                      -- 父级ID
    ,class_name                     -- 分类名称
    ,class_img                      -- 图片路径
    ,type_id                        -- 类型0-苗木分类,1-农资分类
    ,class_level                    -- 类型等级(0-大类,1-小类)
    ,class_show                     -- 是否显示(0-显示,1-隐藏)
    ,class_location                 -- 排序字段
    ,create_id                      -- 创建用户id
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_class
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_goods_brand                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_brand                                ==========
====================================================================================================
select 
     brand_id                       -- 品牌id
    ,brand_name                     -- 品牌名称
    ,brand_url                      -- 品牌网址
    ,brand_img                      -- 图片地址
    ,brand_note                     -- 品牌介绍
    ,brand_location                 -- 排序字段
    ,create_id                      -- 创建用户id
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_brand
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_member_icbc                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_icbc                                ==========
====================================================================================================
select 
     icbc_id                        -- 工行认证id
    ,member_id                      -- 会员id
    ,vendor_short_name              -- 企业用户简称
    ,vendor_name                    -- 企业用户名称，开通 E企付担保支付时必 输
    ,vendor_phone                   -- 企业用户联系电话(会员手机号,带过来不可修改)
    ,vendor_email                   -- 企业用户邮箱
    ,province                       -- 所在省 code例130000
    ,city                           -- 所在市 code例130100
    ,county                         -- 所在区县code例130102
    ,address                        -- 详细地址
    ,postcode                       -- 邮政编码
    ,vendor_type                    -- 企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户
    ,cert_type                      -- 企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证
    ,cert_no                        -- 企业用户注册证件号码，
    ,cert_pic                       -- 企业用户注册证件图片
    ,cert_validityl_str             -- 企业用户注册证件有 效期
    ,operator_name                  -- 企业用户联系人姓名
    ,operator_mobile                -- 企业用户联系人手机
    ,operator_email                 -- 企业用户联系人邮箱
    ,operatorId_no                  -- 企业用户联系人身份 证号
    ,`status`                       -- 状态0-未提交,1-审核中,2-审核成功,3-审核失败,4-提交失败
    ,reason                         -- 失败原因
from guomiaozhishu.tb_member_icbc


====================================================================================================
==========          guomiaozhishu.tb_apply_purchase                                       ==========
==========          db_ods_nstc.clife_nstc_ods_apply_purchase                             ==========
====================================================================================================
select 
     purchase_id                    -- 采购id
    ,member_id                      -- 会员id
    ,purchase_num                   -- 采购数量
    ,purchase_price                 -- 价格
    ,purchase_class                 -- 采购品种
    ,purchase_level                 -- (弃用)
    ,address                        -- 收货地址
    ,fromword                       -- 期望货源地
    ,go                             -- 收获地
    ,create_at                     
    ,create_by                      -- 创建者
    ,update_at                     
    ,conent                         -- 内容(选填）
    ,image                          -- 图片地址
    ,del_flag                      
    ,title                          -- 标题
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0发布中 1已结束 2被驳回
    ,scannum                        -- 浏览次数
from guomiaozhishu.tb_apply_purchase
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_apply_supply                                         ==========
==========          db_ods_nstc.clife_nstc_ods_apply_supply                               ==========
====================================================================================================
select 
     suppery_Id                    
    ,member_id                     
    ,goodclass                      -- 品类
    ,supper_price                   -- 供货价格
    ,`level`                        -- 商品等级
    ,fromword                       -- 货源地
    ,go                             -- 收获地
    ,image                          -- 图片
    ,conment                        -- 内容
    ,update_at                      -- 更新时间
    ,createby                       -- 创建者
    ,create_at                      -- 创建时间
    ,del_flag                      
    ,title                          -- 标题
    ,memeber_level                  -- 会员身份
    ,memeber_avator                 -- 头像
    ,member_phone                   -- 手机号
    ,statu                          -- 0采购中   1已结束  2被驳回
    ,scannum                        -- 浏览次数
    ,supperNum                      -- 供货数量
from guomiaozhishu.tb_apply_supply
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


====================================================================================================
==========          guomiaozhishu.tb_storage_lease                                        ==========
==========          db_ods_nstc.clife_nstc_ods_storage_lease                              ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_storage_rent                                         ==========
==========          db_ods_nstc.clife_nstc_ods_storage_rent                               ==========
====================================================================================================
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


====================================================================================================
==========          guomiaozhishu.tb_goods_type                                           ==========
==========          db_ods_nstc.clife_nstc_ods_goods_type                                 ==========
====================================================================================================
select 
     type_id                        -- 类型id
    ,shop_id                        -- 店铺id
    ,type_name                      -- 类型名称
    ,is_physical                    -- 实物商品0-非实物,1-实物
    ,has_spec                       -- 使用规格0-不使用,1-使用
    ,has_param                      -- 使用参数0-不使用,1-使用
    ,has_brand                      -- 关联品牌0-不关联,1-关联
    ,create_id                      -- 创建用户ID
    ,create_at                      -- 创建时间
    ,update_at                      -- 修改时间
    ,del_flag                       -- 删除标记
from guomiaozhishu.tb_goods_type
where date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') 


