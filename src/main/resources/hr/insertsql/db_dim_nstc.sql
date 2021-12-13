===========================
    clife_nstc_dim_township_area
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_township_area partition(part_date)
select 
     id                             -- 'id' 
    ,name_prov                      -- '省' 
    ,code_prov                      -- '省code' 
    ,name_city                      -- '市' 
    ,code_city                      -- '市code' 
    ,name_coun                      -- '区' 
    ,code_coun                      -- '区code' 
    ,name_town                      -- '镇' 
    ,code_town                      -- '镇code'
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_township_area
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 


===========================
    clife_nstc_dim_shop_coupon
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_shop_coupon partition(part_date)
select
     id                             -- 'ID' 
    ,shop_id                        -- '店铺id' 
    ,coupon_name                    -- '优惠券名称' 
    ,`type`                         -- '优惠券类型(1-满减)' 
    ,sub_money                      -- '优惠金额' 
    ,enough_money                   -- '满减满足金额' 
    ,total_num                      -- '总数量' 
    ,limit_sartAt                   -- '使用期限起' 
    ,limit_endAt                    -- '使用期限至' 
    ,limit_number                   -- '允许单人领取数量' 
    ,has_score                      -- '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      -- '所需余额' 
    ,`status`                       -- '状态(0可用,1不可用)' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '更新时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_shop_coupon
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 



===========================
    clife_nstc_dim_goods_product
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_goods_product partition(part_date)
select
     product_id                     -- '产品id' 
    ,batch_id                       -- '批次id' 
    ,sku                            -- 'SKU' 
    ,goods_id                       -- '商品ID' 
    ,product_name                   -- '货品名' 
    ,product_img                    -- '产品图片' 
    ,product_spec                   -- '货品规格' 
    ,product_price                  -- '价格' 
    ,product_post                   -- '期货购买价' 
    ,product_weight                 -- '一包的重量' 
    ,product_buy_min                -- '期货卖出一包的重量' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,store_name                     -- '仓库名' 
    ,product_loading
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_product
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 



===========================
    clife_nstc_dim_goods_brand
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_goods_brand partition(part_date)
select 
     brand_id                       -- '品牌id' 
    ,brand_name                     -- '品牌名称' 
    ,brand_url                      -- '品牌网址' 
    ,brand_img                      -- '图片地址' 
    ,brand_note                     -- '品牌介绍' 
    ,brand_location                 -- '排序字段' 
    ,create_id                      -- '创建用户id' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_brand
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 


===========================
    clife_nstc_dim_goods_class
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_goods_class partition(part_date)
select  
     class_id                       -- '商品分类id' 
    ,parent_id                      -- '父级ID' 
    ,class_name                     -- '分类名称' 
    ,class_img                      -- '图片路径' 
    ,type_id                        -- '类型0-苗木分类,1-农资分类' 
    ,class_level                    -- '类型等级(0-大类,1-小类)' 
    ,class_show                     -- '是否显示(0-显示,1-隐藏)' 
    ,create_id                      -- '创建用户id' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_class
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 


===========================
    clife_nstc_dim_goods_goods
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_goods_goods partition(part_date)
select   
     a.goods_id                       -- '商品id' 
    ,a.shop_id                        -- '店铺id' 
    ,d.shop_name                      -- '店铺名称' 
    ,a.goods_name                     -- '商品名称' 
    ,a.goods_title                    -- '商品标题' 
    ,a.class_id                       -- '商品分类' 
    ,b.class_name                     -- '分类名称' 
    ,b.parent_id                      -- '父级ID' 
    ,b.class_level                    -- '类型等级(0-大类,1-小类)' 
    ,a.type_id                        -- '商品类型' 
    ,e.type_name
    ,a.goods_img                      -- '商品主图' 
    ,a.goods_note                     -- '0-农资商品,1-苗木商品' 
    ,a.goods_prop                     -- '商品等级' 
    ,a.goods_spec                     -- '规格详情' 
    ,a.goods_param                    -- '参数详情' 
    ,a.has_spec                       -- '启用规格' 
    ,a.goods_unit                     -- '计量单位' 
    ,a.goods_loading                  -- '是否下架(0-待上架，1-上架,2下架，3-待审核,4-拒绝5-卖出)' 
    ,a.recommend                      -- '商品推荐0-不推荐,1-推荐' 
    ,a.recom_xq                       -- '邮费,0-包邮,1-不包邮' 
    ,a.recom_gwc                      -- '挂牌0-普通,1-挂牌' 
    ,a.brand_id                       -- '商品品牌' 
    ,c.brand_name                     -- '品牌名称' 
    ,c.brand_url                      -- '品牌网址' 
    ,a.create_at                      -- '创建时间' 
    ,a.update_at                      -- '修改时间' 
    ,a.part_date                      -- '分区日期'
from (
    select 
         goods_id                     -- '商品id' 
        ,shop_id                      -- '店铺id' 
        ,goods_name                   -- '商品名称' 
        ,goods_title                  -- '商品标题' 
        ,class_id                     -- '商品分类' 
        ,type_id                      -- '商品类型' 
        ,goods_img                    -- '商品主图' 
        ,goods_note                   -- '0-农资商品,1-苗木商品' 
        ,goods_prop                   -- '商品等级' 
        ,goods_spec                   -- '规格详情' 
        ,goods_param                  -- '参数详情' 
        ,has_spec                     -- '启用规格' 
        ,goods_unit                   -- '计量单位' 
        ,goods_loading                -- '是否下架(0-待上架，1-上架,2下架，3-待审核,4-拒绝5-卖出)' 
        ,recommend                    -- '商品推荐0-不推荐,1-推荐' 
        ,recom_xq                     -- '邮费,0-包邮,1-不包邮' 
        ,recom_gwc                    -- '挂牌0-普通,1-挂牌' 
        ,brand_id                     -- '商品品牌' 
        ,create_at                    -- '创建时间' 
        ,update_at                    -- '修改时间'
        ,part_date                    -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_goods_goods
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
) a 
left join db_dw_nstc.clife_nstc_dim_goods_class b 
on a.part_date = b.part_date and a.class_id = b.class_id
left join db_dw_nstc.clife_nstc_dim_goods_brand c 
on a.part_date = b.part_date and a.brand_id = c.brand_id
left join db_dw_nstc.clife_nstc_dim_shop_shop d 
on a.part_date = d.part_date and a.shop_id = d.shop_id
left join db_dw_nstc.clife_nstc_dim_goods_type e 
on a.part_date = e.part_date and a.type_id = e.type_id


===========================
    clife_nstc_dim_member_address
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_member_address partition(part_date)
select   
     address_id                     -- '收货地址id' 
    ,member_id                      -- '会员ID' 
    ,province                       -- '所在省' 
    ,city                           -- '所在市' 
    ,area                           -- '所在县区' 
    ,town                           -- '所在街道' 
    ,address                        -- '详细地址' 
    ,post_code                      -- '邮政编码' 
    ,full_name                      -- '收货人姓名' 
    ,phone                          -- '收货人电话' 
    ,default_value                  -- '是否默认0-非默认,-1默认' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_member_address
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 


===========================
    clife_nstc_dim_shop_shop
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_shop_shop partition(part_date)
select    
     a.shop_id                        -- '店铺信息id' 
    ,a.shop_main                      -- '主营品种' 
    ,a.shop_user_id                   -- '商户ID' 
    ,b.`name`                         -- '商家昵称' 
    ,a.shop_name                      -- '店铺名称' 
    ,a.shop_logo                      -- '店铺图片' 
    ,a.shop_background                -- '背景图' 
    ,a.shop_desc                      -- '店铺简介' 
    ,a.shop_notice                    -- '店铺介绍内容' 
    ,a.shop_type                      -- '认证类型0-个人，1-企业，2-经纪人,10-未认证' 
    ,a.shop_phone                     -- '店铺手机号' 
    ,a.shop_province                  -- '店铺所在省' 
    ,a.shop_city                      -- '店铺所在城市' 
    ,a.shop_area                      -- '店铺所在区域' 
    ,a.shop_town                      -- '乡镇' 
    ,a.shop_address                   -- '经营地址' 
    ,a.shop_star                      -- '信誉值(默认80)' 
    ,a.longitude                      -- '经度' 
    ,a.latitude                       -- '纬度' 
    ,a.create_id                      -- '操作人' 
    ,a.create_at                      -- '创建时间'   
    ,recommend
    ,shop_check
    ,a.part_date                      -- '分区日期'    
from (
    select 
         shop_id                        -- '店铺信息id' 
        ,shop_main                      -- '主营品种' 
        ,shop_user_id                   -- '商户ID' 
        ,shop_name                      -- '店铺名称' 
        ,shop_logo                      -- '店铺图片' 
        ,shop_background                -- '背景图' 
        ,shop_desc                      -- '店铺简介' 
        ,shop_notice                    -- '店铺介绍内容' 
        ,shop_type                      -- '认证类型0-个人，1-企业，2-经纪人,10-未认证' 
        ,shop_phone                     -- '店铺手机号' 
        ,shop_province                  -- '店铺所在省' 
        ,shop_city                      -- '店铺所在城市' 
        ,shop_area                      -- '店铺所在区域' 
        ,shop_town                      -- '乡镇' 
        ,shop_address                   -- '经营地址' 
        ,shop_star                      -- '信誉值(默认80)' 
        ,longitude                      -- '经度' 
        ,latitude                       -- '纬度' 
        ,create_id                      -- '操作人' 
        ,create_at                      -- '创建时间'
        ,part_date                      -- '分区日期'
        ,recommend
        ,shop_check
    from db_ods_nstc.clife_nstc_ods_shop_shop 
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 and disabled = 0
) a 
left join db_ods_nstc.clife_nstc_ods_shop_user b 
on a.part_date = b.part_date and a.shop_user_id = b.shop_user_id and b.del_flag = 0 -- and b.disabled = 0



===========================
    clife_nstc_dim_member_user
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_member_user partition(part_date)
select   
     a.member_id                      -- '会员id' 
    ,a.member_phone                   -- '手机号' 
    ,a.member_name                    -- '昵称' 
    ,a.member_avator                  -- '头像' 
    ,a.intro_user                     -- '推荐人' 
    ,a.`level`                        -- '身份0-个人,1-企业,2-经纪人,10-未选择' 
    ,a.unionid                        -- '微信unionid' 
    ,a.openid                         -- '微信openId' 
    ,a.wx_name                        -- '微信昵称' 
    ,a.ali_name                       -- '阿里名字' 
    ,a.ali_account                    -- '阿里手机号' 
    ,b.icbc_id                        -- '工行认证id' 
    ,b.vendor_short_name              -- '企业用户简称' 
    ,b.vendor_email                   -- '企业用户邮箱' 
    ,b.province                       -- '所在省 code例130000' 
    ,b.city                           -- '所在市 code例130100' 
    ,b.county                         -- '所在区县code例130102' 
    ,b.address                        -- '详细地址' 
    ,b.postcode                       -- '邮政编码' 
    ,b.vendor_type                    -- '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户' 
    ,b.cert_type                      -- '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证' 
    ,b.cert_no                        -- '企业用户注册证件号码，' 
    ,b.cert_pic                       -- '企业用户注册证件图片' 
    ,b.cert_validityl_str             -- '企业用户注册证件有 效期' 
    ,a.create_at                      -- '注册时间' 
    ,a.update_at                      -- '更新时间' 
    ,a.part_date
from (
select 
     member_id                      -- '会员id' 
    ,member_phone                   -- '手机号' 
    ,member_name                    -- '昵称' 
    ,member_avator                  -- '头像' 
    ,intro_user                     -- '推荐人' 
    ,`level`                        -- '身份0-个人,1-企业,2-经纪人,10-未选择' 
    ,unionid                        -- '微信unionid' 
    ,openid                         -- '微信openId' 
    ,wx_name                        -- '微信昵称' 
    ,ali_name                       -- '阿里名字' 
    ,ali_account                    -- '阿里手机号' 
    ,create_at                      -- '注册时间' 
    ,update_at                      -- '更新时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_member_user
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 and disabled = 0
) a 
left join db_ods_nstc.clife_nstc_ods_member_icbc b 
on a.part_date = b.part_date and a.member_id = b.member_id  



===========================
    clife_nstc_dim_goods_type
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_goods_type partition(part_date)
select 
     type_id                        -- '类型id' 
    ,shop_id                        -- '店铺id' 
    ,type_name                      -- '类型名称' 
    ,is_physical                    -- '实物商品0-非实物,1-实物' 
    ,has_spec                       -- '使用规格0-不使用,1-使用' 
    ,has_param                      -- '使用参数0-不使用,1-使用' 
    ,has_brand                      -- '关联品牌0-不关联,1-关联' 
    ,create_id                      -- '创建用户ID' 
    ,create_at                      -- '创建时间' 
    ,update_at                      -- '修改时间' 
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_goods_type
where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0



===========================
    clife_nstc_dim_shop_user
===========================
insert overwrite table db_dw_nstc.clife_nstc_dim_shop_user partition(part_date)
select 
     shop_user_id                   -- 商家账号id
    ,shop_id                        -- 商家id
    ,`level`                        -- 代理等级
    ,phone                          -- 手机号
    ,`name`                         -- 昵称
    ,avator                         -- 头像
    ,intro_user                     -- 推荐人
    ,disabled                       -- 是否冻结账号(0-正常,1-冻结)
    ,create_at                      -- 操作时间
    ,update_at                      -- 修改时间
    ,regexp_replace(date_sub(current_date(),1),'-','') as part_date
from db_ods_nstc.clife_nstc_ods_shop_user
where part_date=regexp_replace(date_sub(current_date(),1),'-','')
  and del_flag=0


