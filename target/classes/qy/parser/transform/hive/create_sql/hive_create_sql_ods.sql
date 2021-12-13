====================================================================================================
==========          guomiaozhishu.tb_township_area                                        ==========
==========          db_ods_nstc.clife_nstc_ods_township_area                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_township_area ( 
     id                             int                  comment 'id' 
    ,name_prov                      string               comment '省' 
    ,code_prov                      string               comment '省code' 
    ,name_city                      string               comment '市' 
    ,code_city                      string               comment '市code' 
    ,name_coun                      string               comment '区' 
    ,code_coun                      string               comment '区code' 
    ,name_town                      string               comment '镇' 
    ,code_town                      string               comment '镇code' 
) comment '省市区镇' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_shop_user                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_user                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_user ( 
     shop_user_id                   string               comment '商家账号id' 
    ,shop_id                        string               comment '商家id' 
    ,`level`                        int                  comment '代理等级' 
    ,phone                          string               comment '手机号' 
    ,`password`                     string               comment '密码' 
    ,token                          string               comment 'token' 
    ,`name`                         string               comment '昵称' 
    ,avator                         string               comment '头像' 
    ,intro_user                     string               comment '推荐人' 
    ,aurora_id                      string               comment '极光推送id' 
    ,disabled                       tinyint              comment '是否冻结账号(0-正常,1-冻结)' 
    ,create_at                      string               comment '操作时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商家账号表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_shop_shop                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_shop                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_shop ( 
     shop_id                        string               comment '店铺信息id' 
    ,shop_main                      string               comment '主营品种' 
    ,shop_user_id                   string               comment '商户ID' 
    ,shop_name                      string               comment '店铺名称' 
    ,shop_logo                      string               comment '店铺图片' 
    ,shop_background                string               comment '背景图' 
    ,shop_desc                      string               comment '店铺简介' 
    ,shop_notice                    string               comment '店铺介绍内容' 
    ,shop_type                      int                  comment '认证类型0-个人，1-企业，2-经纪人,10-未认证' 
    ,shop_phone                     string               comment '店铺手机号' 
    ,shop_province                  string               comment '店铺所在省' 
    ,shop_city                      string               comment '店铺所在城市' 
    ,shop_area                      string               comment '店铺所在区域' 
    ,shop_town                      string               comment '乡镇' 
    ,shop_address                   string               comment '经营地址' 
    ,shop_sale                      int                  comment '总销量' 
    ,shop_star                      double               comment '信誉值(默认80)' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,recommend                      int                  comment '首页展示0-不展示,1-展示' 
    ,hits                           int                  comment '浏览量' 
    ,shop_check                     int                  comment '审核0-审核中,1-成功,2-失败' 
    ,money                          double               comment '账户余额' 
    ,freeze                         double               comment '冻结金额' 
    ,all_money                      double               comment '总成交额' 
    ,bank_card                      string               comment '银行卡号' 
    ,bank_user_name                 string               comment '持卡人姓名' 
    ,bank_name                      string               comment '银行名字' 
    ,bank_address                   string               comment '开户行地址' 
    ,intro_user                     string               comment '推荐人' 
    ,ali_number                     string               comment '支付宝账号' 
    ,ali_name                       string               comment '支付宝名字' 
    ,times                          int                  comment '发货时间' 
    ,location                       int                  comment '排序' 
    ,create_id                      string               comment '操作人' 
    ,create_at                      string               comment '创建时间' 
    ,disabled                       int                  comment '是否禁用(0正常)' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,batchId                        int                 
) comment '店铺' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_shop_coupon                                          ==========
==========          db_ods_nstc.clife_nstc_ods_shop_coupon                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_shop_coupon ( 
     id                             string               comment 'ID' 
    ,shop_id                        string               comment '店铺id' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,`type`                         int                  comment '优惠券类型(1-满减)' 
    ,sub_money                      double               comment '优惠金额' 
    ,enough_money                   double               comment '满减满足金额' 
    ,total_num                      int                  comment '总数量' 
    ,send_num                       int                  comment '已发数量' 
    ,limit_sartAt                   string               comment '使用期限起' 
    ,limit_endAt                    string               comment '使用期限至' 
    ,limit_number                   int                  comment '允许单人领取数量' 
    ,has_score                      int                  comment '发送方式（0：余额购买，1：免费发放）' 
    ,buy_money                      double               comment '所需余额' 
    ,`status`                       int                  comment '状态(0可用,1不可用)' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '全局优惠券' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_record_shop_member                                   ==========
==========          db_ods_nstc.clife_nstc_ods_record_shop_member                         ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_record_shop_member ( 
     id                             bigint               comment '店铺访问记录' 
    ,member_id                      string               comment '会员id' 
    ,shop_id                        string               comment '店铺id' 
    ,`type`                         int                  comment '新老访客0-新访客,1-老访客' 
    ,create_at                      string               comment '访问时间' 
    ,create_day                     string               comment '访问日期' 
) comment '店铺访问记录' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_record_goods_member                                  ==========
==========          db_ods_nstc.clife_nstc_ods_record_goods_member                        ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_record_goods_member ( 
     id                             bigint               comment '访问记录id' 
    ,member_id                      string               comment '会员id' 
    ,goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,`type`                         int                  comment '新老访客数0-新访客,1-老访客' 
    ,create_at                      string               comment '访问时间' 
    ,create_day                     string               comment '访问日期' 
) comment '商品访问记录' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_qa_question                                          ==========
==========          db_ods_nstc.clife_nstc_ods_qa_question                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_qa_question ( 
     question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '会员id' 
    ,question_content               string               comment '问题描述' 
    ,question_img                   string               comment '图片链接' 
    ,`status`                       int                  comment '显示状态0-显示,1-隐藏' 
    ,like_num                       int                  comment '点赞数' 
    ,create_at                      string               comment '发布时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-问题表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_qa_answer                                            ==========
==========          db_ods_nstc.clife_nstc_ods_qa_answer                                  ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_qa_answer ( 
     answer_id                      bigint               comment '答案id' 
    ,question_id                    bigint               comment '问题id' 
    ,member_id                      string               comment '回答人id' 
    ,answer_content                 string               comment '回答内容' 
    ,`status`                       int                  comment '显示状态0-显示,1-隐藏' 
    ,like_num                       int                  comment '回复点赞数' 
    ,create_at                      string               comment '回答时间' 
    ,del_flag                       int                  comment '删除标记0-正常' 
) comment '问答-答案表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_shipping                                       ==========
==========          db_ods_nstc.clife_nstc_ods_order_shipping                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_shipping ( 
     shipping_id                    string               comment '订单收货信息id' 
    ,order_id                       string               comment '订单ID' 
    ,receiver_name                  string               comment '收件人' 
    ,receiver_phone                 string               comment '电话' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,receiver_address               string               comment '地址' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '订单配送表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_order                                          ==========
==========          db_ods_nstc.clife_nstc_ods_order_order                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_order ( 
     order_id                       string               comment '订单id' 
    ,shop_id                        string               comment '店铺id' 
    ,member_id                      string               comment '会员ID' 
    ,order_price                    double               comment '订单总额' 
    ,payment                        double               comment '实付金额' 
    ,order_sn                       string               comment '订单号' 
    ,merge_sn                       string               comment '合单号' 
    ,order_post                     double               comment '邮费' 
    ,coupon_id                      string               comment '优惠券id' 
    ,`status`                       int                  comment '订单状态(0-商议订单，1：待支付，2：待发货，3：待收货，4-待评价，5：退货中，6.退货成功，7：订单完成，8：订单取消）' 
    ,create_at                      string               comment '下单时间' 
    ,update_at                      string               comment '订单更新时间' 
    ,return_at                      string               comment '退货时间' 
    ,pay_at                         string               comment '支付时间' 
    ,pay_type                       int                  comment '0-默认未支付1：微信支付，2：支付宝支付 ,3小程序支付 4工行app支付 5 工行pc支付' 
    ,consign_at                     string               comment '发货时间' 
    ,receiving_at                   string               comment '收货时间' 
    ,end_at                         string               comment '交易完成时间' 
    ,close_at                       string               comment '交易关闭时间' 
    ,coment                         int                  comment '是否评价 0：未评价，1：已评价' 
    ,shipping_type                  string               comment '物流类型0-公司,1-个人' 
    ,shipping_name                  string               comment '物流名称' 
    ,shipping_code                  string               comment '物流单号' 
    ,shipping_reminder              string               comment '发货提醒0-未提醒,1-已提醒' 
    ,`read`                         int                  comment '0未读 1已读' 
    ,buyer_msg                      string               comment '买家留言' 
    ,buyer_nick                     string               comment '买家昵称' 
    ,`export`                       int                  comment '是否已经导出  0：未导出， 1：已导出' 
    ,del_flag                       int                  comment '删除标记' 
) comment '订单信息表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_detail                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_detail                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_detail ( 
     detail_id                      string               comment '订单详情id' 
    ,order_id                       string               comment '订单ID' 
    ,goods_id                       string               comment '商品ID' 
    ,product_id                     string               comment '产品规格ID' 
    ,order_sn                       string               comment '子订单号' 
    ,total                          int                  comment '购买数量' 
    ,coment                         int                  comment '0未评价  1已评价' 
    ,price                          double               comment '单价' 
    ,all_price                      double               comment '总价' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '订单详情表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_coment                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_coment                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_coment ( 
     comment_id                     string               comment '评论表id' 
    ,detail_id                      string               comment '订单详情表id' 
    ,goods_id                       string               comment '商品id' 
    ,product_id                     string               comment '产品id' 
    ,member_id                      string               comment '评论人' 
    ,star                           int                  comment '评价星级 1-5,默认5星' 
    ,content                        string               comment '评价内容' 
    ,comment_img                    string               comment '评论图片' 
    ,disply                         int                  comment '删除标记 0：未删除，1：删除' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '评价表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_back                                           ==========
==========          db_ods_nstc.clife_nstc_ods_order_back                                 ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_back ( 
     back_id                        string               comment '退换货id' 
    ,order_id                       string               comment '订单ID' 
    ,price                          double               comment '退款金额' 
    ,express_name                   string               comment '物流名/司机电话' 
    ,express_code                   string               comment '快物流单号/车牌号' 
    ,`type`                         int                  comment '类型0-公司物流,1-个人找车' 
    ,`status`                       int                  comment '状态0-审核中,1-同意,2-拒绝' 
    ,order_status                   int                  comment '订单原状态' 
    ,reason                         string               comment '退货原因' 
    ,back_type                      int                  comment '0-未发货退款、1-已发货未收到货退款、2-收到货仅退款、3-收到货退货退款' 
    ,reason_f                       string               comment '驳回原因' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '退货记录表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_order_appeal                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_appeal                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_order_appeal ( 
     id                             int                  comment '申诉id' 
    ,order_id                       string               comment '订单id' 
    ,reason_content                 string               comment '申诉原因' 
    ,reason_img                     string               comment '申诉图片' 
    ,`status`                       string               comment '处理状态0-未处理,1-已处理' 
    ,create_at                      string               comment '申诉时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '申诉表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_user                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_user                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_user ( 
     member_id                      string               comment '会员id' 
    ,member_phone                   string               comment '手机号' 
    ,member_name                    string               comment '昵称' 
    ,member_avator                  string               comment '头像' 
    ,`password`                     string               comment '密码' 
    ,token                          string               comment 'token' 
    ,`status`                       int                  comment '工行E企付状态0-未提交,1-工行审核中,2-审核通过,3-审核拒绝' 
    ,intro_user                     string               comment '推荐人' 
    ,`level`                        int                  comment '身份0-个人,1-企业,2-经纪人,10-未选择' 
    ,money                          double               comment '余额' 
    ,freeze                         double               comment '数字金额' 
    ,aurora_id                      string               comment '极光推送id' 
    ,create_at                      string               comment '注册时间' 
    ,update_at                      string               comment '更新时间' 
    ,unionid                        string               comment '微信unionid' 
    ,openid                         string               comment '微信openId' 
    ,xcxopenid                      string               comment '小程序openid' 
    ,wx_name                        string               comment '微信昵称' 
    ,ali_name                       string               comment '阿里名字' 
    ,ali_account                    string               comment '阿里手机号' 
    ,disabled                       tinyint              comment '是否冻结账号(0-正常,1-冻结)（无用）' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,icbc_status                    int                  comment '工行会员认证0-未认证,1-审核中,2-已认证,3-审核失败,4-提交失败' 
) comment '会员表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_shop                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_shop                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_shop ( 
     id                             bigint              
    ,shop_id                        string               comment '店铺id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '关注时间' 
) comment '店铺关注表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_question                                      ==========
==========          db_ods_nstc.clife_nstc_ods_member_question                            ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_question ( 
     member_id                      string               comment '会员id' 
    ,question_id                    bigint               comment '问题id' 
) comment '问题点赞表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_follow                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_follow                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_follow ( 
     follow_id                      bigint              
    ,goods_id                       string               comment '商品id' 
    ,member_id                      string               comment '会员id' 
    ,create_at                      string               comment '创建时间' 
) comment '商品关注表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_coupon                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_coupon                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_coupon ( 
     id                             string               comment 'ID' 
    ,member_id                      string               comment '会员ID' 
    ,coupon_id                      string               comment '优惠券ID' 
    ,`type`                         int                  comment '优惠券类型（1 满减 ）' 
    ,coupon_name                    string               comment '优惠券名称' 
    ,sub_money                      double               comment '优惠金额' 
    ,coupon_money                   double               comment '满减满足金额' 
    ,order_id                       string               comment '订单ID' 
    ,`status`                       int                  comment '优惠券状态（0-未使用，1-已使用，2-已过期，3-已失效）' 
    ,create_at                      string               comment '获取时间' 
    ,order_at                       string               comment '使用时间' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '用户优惠券' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_answer                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_answer                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_answer ( 
     member_id                      string               comment '会员id' 
    ,answer_id                      bigint               comment '回复id' 
) comment '回复点赞表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_address                                       ==========
==========          db_ods_nstc.clife_nstc_ods_member_address                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_address ( 
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
    ,default_value                  int                  comment '是否默认0-非默认,-1默认' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '收货地址' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_goods_product                                        ==========
==========          db_ods_nstc.clife_nstc_ods_goods_product                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_product ( 
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
    ,product_stock                  int                  comment '库存' 
    ,product_buy_min                int                  comment '期货卖出一包的重量' 
    ,product_buy_max                int                  comment '期货卖出库存' 
    ,product_unit                   string               comment '计量单位' 
    ,product_loading                int                  comment '是否下架(0审核1-上架2下架,3-拒绝,5-卖出)' 
    ,product_default                int                  comment '是否默认(1是默认)' 
    ,loading_at                     string               comment '上架时间' 
    ,unloading__at                  string               comment '下架时间' 
    ,product_sale_num               int                  comment '销售量' 
    ,product_location               int                  comment '排序字段' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
    ,store_name                     string               comment '仓库名' 
) comment '商品产品' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_goods_goods                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_goods                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_goods ( 
     goods_id                       string               comment '商品id' 
    ,shop_id                        string               comment '店铺id' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,type_id                        string               comment '商品类型' 
    ,goods_img                      string               comment '商品主图' 
    ,goods_note                     int                  comment '0-农资商品,1-苗木商品' 
    ,goods_prop                     string               comment '商品等级' 
    ,goods_spec                     string               comment '规格详情' 
    ,goods_param                    string               comment '参数详情' 
    ,has_spec                       int                  comment '启用规格' 
    ,goods_unit                     string               comment '计量单位' 
    ,goods_loading                  int                  comment '是否下架(0-待上架，1-上架,2下架，3-待审核,4-拒绝5-卖出)' 
    ,examine                        int                  comment '审核状态0-未审核,1-审核中,2-已通过,3-拒绝' 
    ,goods_reject                   string               comment '驳回原因' 
    ,goods_view_num                 int                  comment '浏览量' 
    ,goods_comment_num              int                  comment '评论量' 
    ,goods_sale_num                 int                  comment '销售量' 
    ,goods_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记0-显示,1-删除' 
    ,recom_sy                       int                  comment '0-优品优选,1-乡村振兴' 
    ,recommend                      string               comment '商品推荐0-不推荐,1-推荐' 
    ,recom_xq                       int                  comment '邮费,0-包邮,1-不包邮' 
    ,recom_gwc                      int                  comment '挂牌0-普通,1-挂牌' 
    ,brand_id                       string               comment '商品品牌' 
    ,unloading_at                   string               comment '下架时间' 
    ,loading_at                     string               comment '上架时间' 
) comment '商品信息表' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_goods_class                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_class                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_class ( 
     class_id                       bigint               comment '商品分类id' 
    ,parent_id                      bigint               comment '父级ID' 
    ,class_name                     string               comment '分类名称' 
    ,class_img                      string               comment '图片路径' 
    ,type_id                        string               comment '类型0-苗木分类,1-农资分类' 
    ,class_level                    tinyint              comment '类型等级(0-大类,1-小类)' 
    ,class_show                     tinyint              comment '是否显示(0-显示,1-隐藏)' 
    ,class_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '商品分类' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_goods_brand                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_brand                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_brand ( 
     brand_id                       string               comment '品牌id' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,brand_img                      string               comment '图片地址' 
    ,brand_note                     string               comment '品牌介绍' 
    ,brand_location                 int                  comment '排序字段' 
    ,create_id                      string               comment '创建用户id' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       tinyint              comment '删除标记' 
) comment '商品品牌' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_member_icbc                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_icbc                                ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_member_icbc ( 
     icbc_id                        int                  comment '工行认证id' 
    ,member_id                      string               comment '会员id' 
    ,vendor_short_name              string               comment '企业用户简称' 
    ,vendor_name                    string               comment '企业用户名称，开通 E企付担保支付时必 输' 
    ,vendor_phone                   string               comment '企业用户联系电话(会员手机号,带过来不可修改)' 
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
    ,operator_name                  string               comment '企业用户联系人姓名' 
    ,operator_mobile                string               comment '企业用户联系人手机' 
    ,operator_email                 string               comment '企业用户联系人邮箱' 
    ,operatorId_no                  string               comment '企业用户联系人身份 证号' 
    ,`status`                       int                  comment '状态0-未提交,1-审核中,2-审核成功,3-审核失败,4-提交失败' 
    ,reason                         string               comment '失败原因' 
) comment '工行支付认证' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_apply_purchase                                       ==========
==========          db_ods_nstc.clife_nstc_ods_apply_purchase                             ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_apply_purchase ( 
     purchase_id                    int                  comment '采购id' 
    ,member_id                      string               comment '会员id' 
    ,purchase_num                   int                  comment '采购数量' 
    ,purchase_price                 double               comment '价格' 
    ,purchase_class                 string               comment '采购品种' 
    ,purchase_level                 string               comment '(弃用)' 
    ,address                        string               comment '收货地址' 
    ,fromword                       string               comment '期望货源地' 
    ,go                             string               comment '收获地' 
    ,create_at                      string              
    ,create_by                      string               comment '创建者' 
    ,update_at                      string              
    ,conent                         string               comment '内容(选填）' 
    ,image                          string               comment '图片地址' 
    ,del_flag                       string              
    ,title                          string               comment '标题' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0发布中 1已结束 2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
) comment '采购信息' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_apply_supply                                         ==========
==========          db_ods_nstc.clife_nstc_ods_apply_supply                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_apply_supply ( 
     suppery_Id                     bigint              
    ,member_id                      string              
    ,goodclass                      string               comment '品类' 
    ,supper_price                   string               comment '供货价格' 
    ,`level`                        string               comment '商品等级' 
    ,fromword                       string               comment '货源地' 
    ,go                             string               comment '收获地' 
    ,image                          string               comment '图片' 
    ,conment                        string               comment '内容' 
    ,update_at                      string               comment '更新时间' 
    ,createby                       string               comment '创建者' 
    ,create_at                      string               comment '创建时间' 
    ,del_flag                       string              
    ,title                          string               comment '标题' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
    ,supperNum                      int                  comment '供货数量' 
) comment '供应信息' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_storage_lease                                        ==========
==========          db_ods_nstc.clife_nstc_ods_storage_lease                              ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_storage_lease ( 
     lease_id                       bigint               comment '用车id' 
    ,member_id                      string               comment '会员id' 
    ,title                          string               comment '标题' 
    ,price                          double               comment '价格' 
    ,local_no                       string               comment '冷库库位' 
    ,stcity                         string               comment '起运城市' 
    ,address                        string               comment '起运地详细地址' 
    ,encity                         string               comment '目的城市' 
    ,area                           string               comment '目的地地址' 
    ,content                        string               comment '备注' 
    ,image                          string               comment '图片' 
    ,create_at                      string               comment '发布时间' 
    ,create_by                      string               comment '创建人' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       string               comment '删除标记' 
) comment '我要用车' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_storage_rent                                         ==========
==========          db_ods_nstc.clife_nstc_ods_storage_rent                               ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_storage_rent ( 
     rent_id                        int                  comment '出租拉货id' 
    ,title                          string               comment '标题' 
    ,price                          double               comment '价格' 
    ,area                           string               comment '货车类型' 
    ,stcity                         string               comment '起运城市' 
    ,staddress                      string               comment '起运地址' 
    ,encity                         string               comment '目的城市' 
    ,address                        string               comment '目的地范围' 
    ,conent                         string               comment '备注内容' 
    ,image                          string               comment '图片' 
    ,create_at                      string               comment '发布时间' 
    ,create_by                      string               comment '创造者' 
    ,member_id                      string               comment '会员id' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览量' 
    ,longitude                      string               comment '经度' 
    ,latitude                       string               comment '纬度' 
    ,localNo                        string               comment '库位' 
    ,update_at                      string               comment '更新时间' 
    ,del_flag                       string               comment '删除标记' 
) comment '我要拉货' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          guomiaozhishu.tb_goods_type                                           ==========
==========          db_ods_nstc.clife_nstc_ods_goods_type                                 ==========
====================================================================================================
create table if not exists db_ods_nstc.clife_nstc_ods_goods_type ( 
     type_id                        string               comment '类型id' 
    ,shop_id                        string               comment '店铺id' 
    ,type_name                      string               comment '类型名称' 
    ,is_physical                    int                  comment '实物商品0-非实物,1-实物' 
    ,has_spec                       int                  comment '使用规格0-不使用,1-使用' 
    ,has_param                      int                  comment '使用参数0-不使用,1-使用' 
    ,has_brand                      int                  comment '关联品牌0-不关联,1-关联' 
    ,create_id                      string               comment '创建用户ID' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '修改时间' 
    ,del_flag                       int                  comment '删除标记' 
) comment '商品类型' 
partitioned by ( part_date string ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


