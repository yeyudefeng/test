====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_date                                        ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_date ( 
     dt_code                        string               comment '日期代码' 
    ,date_s                         string               comment '日期代码/' 
    ,date_w                         string               comment '日期代码-' 
    ,date_cn                        string               comment '日期代码-CN' 
    ,y_code                         string               comment '年份代码' 
    ,q_code                         string               comment '季度代码' 
    ,q_code_cn                      string               comment '季度-CN' 
    ,m_code                         string               comment '月份代码' 
    ,m_code_cn                      string               comment '月份-CN' 
    ,week_of_month_cn               string               comment '本月第几周' 
    ,week_of_year_cn                string               comment '本年第几周' 
    ,d_code                         string               comment '日' 
    ,day_of_year_cn                 string               comment '本年第几天' 
    ,week_cn                        string               comment '星期几' 
) comment '时间维表' 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_shop_user                                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,disabled                       int                  comment '是否冻结账号(0-正常' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商家账号维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_township_area                               ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_township_area ( 
     id                             int                  comment 'id' 
    ,name_prov                      string               comment '省' 
    ,code_prov                      string               comment '省code' 
    ,name_city                      string               comment '市' 
    ,code_city                      string               comment '市code' 
    ,name_coun                      string               comment '区' 
    ,code_coun                      string               comment '区code' 
    ,name_town                      string               comment '镇' 
    ,code_town                      string               comment '镇code' 
) comment '省市区镇维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_shop_coupon                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_coupon ( 
     id                             string               comment 'ID' 
    ,shop_id                        string               comment '店铺id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,`type`                         int                  comment '优惠券类型(1-满减)' 
    ,sub_money                      double               comment '优惠金额' 
    ,enough_money                   double               comment '满减满足金额' 
    ,total_num                      int                  comment '总数量' 
    ,limit_sartAt                   string               comment '使用期限起' 
    ,limit_endAt                    string               comment '使用期限至' 
    ,limit_number                   int                  comment '允许单人领取数量' 
    ,has_score                      int                  comment '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      double               comment '所需余额' 
    ,`status`                       int                  comment '状态(0可用' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
) comment '全局优惠券维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_brand                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_brand ( 
     brand_id                       string               comment '品牌id' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,brand_img                      string               comment '图片地址' 
    ,brand_note                     string               comment '品牌介绍' 
    ,brand_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品品牌维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_class                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_class ( 
     class_id                       bigint               comment '商品分类id' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_name                     string               comment '分类名称' 
    ,class_img                      string               comment '图片路径' 
    ,type_id                        string               comment '类型0-苗木分类' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,class_show                     tinyint              comment '是否显示(0-显示' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品分类维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_member_address                              ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_member_address ( 
     address_id                     int                  comment '收货地址id' 
    ,member_id                      string               comment '会员ID' 
    ,province                       string               comment '所在省' 
    ,city                           string               comment '所在市' 
    ,area                           string               comment '所在县区' 
    ,town                           string               comment '所在街道' 
    ,address                        string               comment '详细地址' 
    ,post_code                      string               comment '邮政编码' 
    ,full_name                      string               comment '收货人姓名' 
    ,phone                          string               comment '收货人电话' 
    ,default_value                  int                  comment '是否默认0-非默认' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '收货地址维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_shop_shop                                   ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_shop_shop ( 
     shop_id                        string               comment '店铺信息id' 
    ,shop_main                      string               comment '主营品种' 
    ,shop_user_id                   string               comment '商户ID' 
    ,`name`                         string               comment '商家昵称' 
    ,shop_name                      string               comment '店铺名称' 
    ,shop_logo                      string               comment '店铺图片' 
    ,shop_background                string               comment '背景图' 
    ,shop_desc                      string               comment '店铺简介' 
    ,shop_notice                    string               comment '店铺介绍内容' 
    ,shop_type                      int                  comment '认证类型0-个人，1-企业，2-经纪人' 
    ,shop_phone                     string               comment '店铺手机号' 
    ,shop_province                  string               comment '店铺所在省' 
    ,shop_city                      string               comment '店铺所在城市' 
    ,shop_area                      string               comment '店铺所在区域' 
    ,shop_town                      string               comment '乡镇' 
    ,shop_address                   string               comment '经营地址' 
    ,shop_star                      double               comment '信誉值(默认80)' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,create_id                      string               comment '操作人' 
    ,create_at                      string               comment '创建时间' 
    ,recommend                      int                  comment '首页展示' 
    ,shop_check                     int                  comment '审核状态' 
) comment '店铺维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_member_user                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_member_user ( 
     member_id                      string               comment '会员id' 
    ,member_phone                   string               comment '手机号' 
    ,member_name                    string               comment '昵称' 
    ,member_avator                  string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,`level`                        int                  comment '身份0-个人' 
    ,unionid                        string               comment '微信unionid' 
    ,openid                         string               comment '微信openId' 
    ,wx_name                        string               comment '微信昵称' 
    ,ali_name                       string               comment '阿里名字' 
    ,ali_account                    string               comment '阿里手机号' 
    ,icbc_id                        int                  comment '工行认证id' 
    ,vendor_short_name              string               comment '企业用户简称' 
    ,vendor_email                   string               comment '企业用户邮箱' 
    ,province                       string               comment '所在省 code例130000' 
    ,city                           string               comment '所在市 code例130100' 
    ,county                         string               comment '所在区县code例130102' 
    ,address                        string               comment '详细地址' 
    ,postcode                       string               comment '邮政编码' 
    ,vendor_type                    string               comment '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户' 
    ,cert_type                      string               comment '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证' 
    ,cert_no                        string               comment '企业用户注册证件号码，' 
    ,cert_pic                       string               comment '企业用户注册证件图片' 
    ,cert_validityl_str             string               comment '企业用户注册证件有 效期' 
    ,create_at                      string               comment '注册时间' 
    ,update_at                      string               comment '更新时间' 
) comment '会员维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_type                                  ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_type ( 
     type_id                        string               comment '类型id' 
    ,shop_id                        string               comment '店铺id' 
    ,type_name                      string               comment '类型名称' 
    ,is_physical                    int                  comment '实物商品0-非实物' 
    ,has_spec                       int                  comment '使用规格0-不使用' 
    ,has_param                      int                  comment '使用参数0-不使用' 
    ,has_brand                      int                  comment '关联品牌0-不关联' 
    ,create_id                      string               comment '创建用户ID' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品类型维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_goods                                 ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_goods ( 
     goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,shop_name                      string               comment '店铺名称' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,class_name                     string               comment '分类名称' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_level                    tinyint              comment '类型等级(0-大类' 
    ,type_id                        string               comment '商品类型' 
    ,type_name                      string               comment '类型名称' 
    ,goods_img                      string               comment '商品主图' 
    ,goods_note                     int                  comment '0-农资商品' 
    ,goods_prop                     string               comment '商品等级' 
    ,goods_spec                     string               comment '规格详情' 
    ,goods_param                    string               comment '参数详情' 
    ,has_spec                       int                  comment '启用规格' 
    ,goods_unit                     string               comment '计量单位' 
    ,goods_loading                  int                  comment '是否下架(0-待上架，1-上架' 
    ,recommend                      string               comment '商品推荐0-不推荐' 
    ,recom_xq                       int                  comment '邮费' 
    ,recom_gwc                      int                  comment '挂牌0-普通' 
    ,brand_id                       string               comment '商品品牌' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
) comment '商品信息表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_nstc.clife_nstc_dim_goods_product                               ==========
====================================================================================================
create table if not exists db_dw_nstc.clife_nstc_dim_goods_product ( 
     product_id                     string               comment '产品id' 
    ,batch_id                       string               comment '批次id' 
    ,sku                            string               comment 'SKU' 
    ,goods_id                       string               comment '商品ID' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,product_weight                 int                  comment '一包的重量' 
    ,product_buy_min                int                  comment '期货卖出一包的重量' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,store_name                     string               comment '仓库名' 
    ,product_loading                int                  comment '是否下架' 
) comment '商品产品维表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


