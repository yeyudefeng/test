====================================================================================================
==========          guomiaozhishu.tb_township_area                                        ==========
==========          db_ods_nstc.clife_nstc_ods_township_area                              ==========
====================================================================================================
CREATE TABLE `tb_township_area` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name_prov` varchar(32) DEFAULT '' COMMENT '省',
  `code_prov` varchar(32) DEFAULT '' COMMENT '省code',
  `name_city` varchar(32) DEFAULT '' COMMENT '市',
  `code_city` varchar(32) DEFAULT '' COMMENT '市code',
  `name_coun` varchar(32) DEFAULT '' COMMENT '区',
  `code_coun` varchar(32) DEFAULT '' COMMENT '区code',
  `name_town` varchar(32) DEFAULT '' COMMENT '镇',
  `code_town` varchar(32) DEFAULT '' COMMENT '镇code',
  PRIMARY KEY (`id`),
  KEY `index_code_prov` (`code_prov`),
  KEY `index_code_city` (`code_city`),
  KEY `index_code_coun` (`code_coun`)
) ENGINE=InnoDB AUTO_INCREMENT=40200 DEFAULT CHARSET=utf8mb4 COMMENT='省市区镇'


====================================================================================================
==========          guomiaozhishu.tb_shop_user                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_user                                  ==========
====================================================================================================
CREATE TABLE `tb_shop_user` (
  `shop_user_id` varchar(32) NOT NULL DEFAULT '' COMMENT '商家账号id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '商家id',
  `level` int(3) DEFAULT '0' COMMENT '代理等级',
  `phone` varchar(16) DEFAULT '' COMMENT '手机号',
  `password` varchar(128) DEFAULT '' COMMENT '密码',
  `token` varchar(32) DEFAULT '' COMMENT 'token',
  `name` varchar(100) DEFAULT '' COMMENT '昵称',
  `avator` varchar(255) DEFAULT '' COMMENT '头像',
  `intro_user` varchar(32) DEFAULT '' COMMENT '推荐人',
  `aurora_id` varchar(255) DEFAULT '' COMMENT '极光推送id',
  `disabled` tinyint(1) DEFAULT '0' COMMENT '是否冻结账号(0-正常,1-冻结)',
  `create_at` datetime DEFAULT NULL COMMENT '操作时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` int(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`shop_user_id`),
  UNIQUE KEY `INDEX_MEMBER_USER_LOGINNAMAE` (`phone`),
  KEY `index_shop_id` (`shop_id`),
  KEY `index_shop_user_phone` (`phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商家账号表'


====================================================================================================
==========          guomiaozhishu.tb_shop_shop                                            ==========
==========          db_ods_nstc.clife_nstc_ods_shop_shop                                  ==========
====================================================================================================
CREATE TABLE `tb_shop_shop` (
  `shop_id` varchar(32) NOT NULL DEFAULT '' COMMENT '店铺信息id',
  `shop_main` varchar(32) DEFAULT NULL COMMENT '主营品种',
  `shop_user_id` varchar(32) DEFAULT NULL COMMENT '商户ID',
  `shop_name` varchar(255) DEFAULT '' COMMENT '店铺名称',
  `shop_logo` varchar(255) DEFAULT '' COMMENT '店铺图片',
  `shop_background` varchar(255) DEFAULT '' COMMENT '背景图',
  `shop_desc` varchar(255) DEFAULT '' COMMENT '店铺简介',
  `shop_notice` varchar(255) DEFAULT '' COMMENT '店铺介绍内容',
  `shop_type` int(2) DEFAULT '10' COMMENT '认证类型0-个人，1-企业，2-经纪人,10-未认证',
  `shop_phone` varchar(32) DEFAULT '' COMMENT '店铺手机号',
  `shop_province` varchar(32) DEFAULT '' COMMENT '店铺所在省',
  `shop_city` varchar(32) DEFAULT '' COMMENT '店铺所在城市',
  `shop_area` varchar(32) DEFAULT '' COMMENT '店铺所在区域',
  `shop_town` varchar(255) DEFAULT NULL COMMENT '乡镇',
  `shop_address` varchar(255) DEFAULT '' COMMENT '经营地址',
  `shop_sale` int(11) DEFAULT '0' COMMENT '总销量',
  `shop_star` decimal(10,2) DEFAULT '80.00' COMMENT '信誉值(默认80)',
  `longitude` varchar(50) DEFAULT NULL COMMENT '经度',
  `latitude` varchar(50) DEFAULT NULL COMMENT '纬度',
  `recommend` int(2) DEFAULT '0' COMMENT '首页展示0-不展示,1-展示',
  `hits` int(11) DEFAULT '0' COMMENT '浏览量',
  `shop_check` int(2) DEFAULT '0' COMMENT '审核0-审核中,1-成功,2-失败',
  `money` decimal(16,2) DEFAULT '0.00' COMMENT '账户余额',
  `freeze` decimal(16,2) DEFAULT '0.00' COMMENT '冻结金额',
  `all_money` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '总成交额',
  `bank_card` varchar(50) DEFAULT '' COMMENT '银行卡号',
  `bank_user_name` varchar(32) DEFAULT '' COMMENT '持卡人姓名',
  `bank_name` varchar(32) DEFAULT '' COMMENT '银行名字',
  `bank_address` varchar(128) DEFAULT '' COMMENT '开户行地址',
  `intro_user` varchar(32) DEFAULT '' COMMENT '推荐人',
  `ali_number` varchar(32) DEFAULT '' COMMENT '支付宝账号',
  `ali_name` varchar(32) DEFAULT '' COMMENT '支付宝名字',
  `times` int(3) DEFAULT '0' COMMENT '发货时间',
  `location` int(10) DEFAULT '0' COMMENT '排序',
  `create_id` varchar(32) DEFAULT '' COMMENT '操作人',
  `create_at` varchar(32) DEFAULT '' COMMENT '创建时间',
  `disabled` int(1) DEFAULT '0' COMMENT '是否禁用(0正常)',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  `batchId` int(11) DEFAULT NULL,
  PRIMARY KEY (`shop_id`),
  KEY `index_shop_phone` (`shop_phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='店铺'


====================================================================================================
==========          guomiaozhishu.tb_shop_coupon                                          ==========
==========          db_ods_nstc.clife_nstc_ods_shop_coupon                                ==========
====================================================================================================
CREATE TABLE `tb_shop_coupon` (
  `id` varchar(32) NOT NULL DEFAULT '' COMMENT 'ID',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `coupon_name` varchar(255) DEFAULT '' COMMENT '优惠券名称',
  `type` int(2) DEFAULT '1' COMMENT '优惠券类型(1-满减)',
  `sub_money` decimal(12,2) DEFAULT '0.00' COMMENT '优惠金额',
  `enough_money` decimal(12,2) DEFAULT '0.00' COMMENT '满减满足金额',
  `total_num` int(32) DEFAULT '0' COMMENT '总数量',
  `send_num` int(32) DEFAULT '0' COMMENT '已发数量',
  `limit_sartAt` datetime DEFAULT NULL COMMENT '使用期限起',
  `limit_endAt` datetime DEFAULT NULL COMMENT '使用期限至',
  `limit_number` int(10) DEFAULT '1' COMMENT '允许单人领取数量',
  `has_score` int(1) DEFAULT '0' COMMENT '发送方式（0：余额购买，1：免费发放）',
  `buy_money` decimal(10,2) DEFAULT '0.00' COMMENT '所需余额',
  `status` int(1) DEFAULT '0' COMMENT '状态(0可用,1不可用)',
  `create_at` varchar(32) DEFAULT '' COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='全局优惠券'


====================================================================================================
==========          guomiaozhishu.tb_record_shop_member                                   ==========
==========          db_ods_nstc.clife_nstc_ods_record_shop_member                         ==========
====================================================================================================
CREATE TABLE `tb_record_shop_member` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '店铺访问记录',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `type` int(2) DEFAULT '0' COMMENT '新老访客0-新访客,1-老访客',
  `create_at` varchar(32) DEFAULT '' COMMENT '访问时间',
  `create_day` varchar(32) DEFAULT '' COMMENT '访问日期',
  PRIMARY KEY (`id`),
  KEY `index_member_id` (`member_id`),
  KEY `index_shop_id` (`shop_id`)
) ENGINE=InnoDB AUTO_INCREMENT=47191 DEFAULT CHARSET=utf8mb4 COMMENT='店铺访问记录'


====================================================================================================
==========          guomiaozhishu.tb_record_goods_member                                  ==========
==========          db_ods_nstc.clife_nstc_ods_record_goods_member                        ==========
====================================================================================================
CREATE TABLE `tb_record_goods_member` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '访问记录id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员id',
  `goods_id` varchar(32) DEFAULT '' COMMENT '商品id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `type` int(2) DEFAULT '0' COMMENT '新老访客数0-新访客,1-老访客',
  `create_at` varchar(32) DEFAULT '' COMMENT '访问时间',
  `create_day` varchar(32) DEFAULT '' COMMENT '访问日期',
  PRIMARY KEY (`id`),
  KEY `index_member_id` (`member_id`),
  KEY `index_shop_id` (`shop_id`),
  KEY `index_goods_id` (`goods_id`)
) ENGINE=InnoDB AUTO_INCREMENT=86065 DEFAULT CHARSET=utf8mb4 COMMENT='商品访问记录'


====================================================================================================
==========          guomiaozhishu.tb_qa_question                                          ==========
==========          db_ods_nstc.clife_nstc_ods_qa_question                                ==========
====================================================================================================
CREATE TABLE `tb_qa_question` (
  `question_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '问题id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员id',
  `question_content` varchar(1000) DEFAULT '' COMMENT '问题描述',
  `question_img` varchar(1000) DEFAULT '' COMMENT '图片链接',
  `status` int(10) DEFAULT '0' COMMENT '显示状态0-显示,1-隐藏',
  `like_num` int(10) DEFAULT '0' COMMENT '点赞数',
  `create_at` varchar(32) DEFAULT '' COMMENT '发布时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记0-正常',
  PRIMARY KEY (`question_id`),
  KEY `index_member_id` (`member_id`)
) ENGINE=InnoDB AUTO_INCREMENT=50 DEFAULT CHARSET=utf8mb4 COMMENT='问答-问题表'


====================================================================================================
==========          guomiaozhishu.tb_qa_answer                                            ==========
==========          db_ods_nstc.clife_nstc_ods_qa_answer                                  ==========
====================================================================================================
CREATE TABLE `tb_qa_answer` (
  `answer_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '答案id',
  `question_id` bigint(20) DEFAULT '0' COMMENT '问题id',
  `member_id` varchar(32) DEFAULT '' COMMENT '回答人id',
  `answer_content` varchar(1000) DEFAULT '' COMMENT '回答内容',
  `status` int(2) DEFAULT '0' COMMENT '显示状态0-显示,1-隐藏',
  `like_num` int(10) DEFAULT '0' COMMENT '回复点赞数',
  `create_at` varchar(32) DEFAULT '' COMMENT '回答时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记0-正常',
  PRIMARY KEY (`answer_id`),
  KEY `index_question_id` (`question_id`) COMMENT 'index_question_id',
  KEY `index_member_id` (`member_id`) COMMENT 'index_member_id'
) ENGINE=InnoDB AUTO_INCREMENT=440 DEFAULT CHARSET=utf8mb4 COMMENT='问答-答案表'


====================================================================================================
==========          guomiaozhishu.tb_order_shipping                                       ==========
==========          db_ods_nstc.clife_nstc_ods_order_shipping                             ==========
====================================================================================================
CREATE TABLE `tb_order_shipping` (
  `shipping_id` varchar(32) NOT NULL DEFAULT '0' COMMENT '订单收货信息id',
  `order_id` varchar(32) DEFAULT '' COMMENT '订单ID',
  `receiver_name` varchar(32) DEFAULT '' COMMENT '收件人',
  `receiver_phone` varchar(32) DEFAULT '' COMMENT '电话',
  `receiver_province` varchar(32) DEFAULT '' COMMENT '省份',
  `receiver_city` varchar(32) DEFAULT '' COMMENT '城市',
  `receiver_area` varchar(32) DEFAULT '' COMMENT '区县',
  `receiver_town` varchar(32) DEFAULT '' COMMENT '街道',
  `receiver_address` varchar(255) DEFAULT '' COMMENT '地址',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`shipping_id`),
  KEY `index_order_id` (`order_id`) COMMENT '订单id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='订单配送表'


====================================================================================================
==========          guomiaozhishu.tb_order_order                                          ==========
==========          db_ods_nstc.clife_nstc_ods_order_order                                ==========
====================================================================================================
CREATE TABLE `tb_order_order` (
  `order_id` varchar(32) NOT NULL DEFAULT '' COMMENT '订单id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员ID',
  `order_price` decimal(10,2) DEFAULT '0.00' COMMENT '订单总额',
  `payment` decimal(10,2) DEFAULT '0.00' COMMENT '实付金额',
  `order_sn` varchar(32) DEFAULT '' COMMENT '订单号',
  `merge_sn` varchar(32) DEFAULT '' COMMENT '合单号',
  `order_post` decimal(10,2) DEFAULT '0.00' COMMENT '邮费',
  `coupon_id` varchar(32) DEFAULT NULL COMMENT '优惠券id',
  `status` int(11) DEFAULT '0' COMMENT '订单状态(0-商议订单，1：待支付，2：待发货，3：待收货，4-待评价，5：退货中，6.退货成功，7：订单完成，8：订单取消）',
  `create_at` varchar(32) DEFAULT '' COMMENT '下单时间',
  `update_at` varchar(32) DEFAULT '' COMMENT '订单更新时间',
  `return_at` varchar(32) DEFAULT '' COMMENT '退货时间',
  `pay_at` varchar(32) DEFAULT '' COMMENT '支付时间',
  `pay_type` int(11) DEFAULT '0' COMMENT '0-默认未支付1：微信支付，2：支付宝支付 ,3小程序支付 4工行app支付 5 工行pc支付',
  `consign_at` varchar(32) DEFAULT '' COMMENT '发货时间',
  `receiving_at` varchar(32) DEFAULT '' COMMENT '收货时间',
  `end_at` varchar(32) DEFAULT '' COMMENT '交易完成时间',
  `close_at` varchar(32) DEFAULT '' COMMENT '交易关闭时间',
  `coment` int(2) DEFAULT '0' COMMENT '是否评价 0：未评价，1：已评价',
  `shipping_type` char(2) DEFAULT '0' COMMENT '物流类型0-公司,1-个人',
  `shipping_name` varchar(32) DEFAULT '' COMMENT '物流名称',
  `shipping_code` varchar(32) DEFAULT '' COMMENT '物流单号',
  `shipping_reminder` varchar(2) DEFAULT '0' COMMENT '发货提醒0-未提醒,1-已提醒',
  `read` int(1) DEFAULT '0' COMMENT '0未读 1已读',
  `buyer_msg` varchar(255) DEFAULT '' COMMENT '买家留言',
  `buyer_nick` varchar(255) DEFAULT '' COMMENT '买家昵称',
  `export` int(1) DEFAULT '0' COMMENT '是否已经导出  0：未导出， 1：已导出',
  `del_flag` int(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`order_id`),
  KEY `index_member_id` (`member_id`) COMMENT '用户id索引',
  KEY `index_shop_id` (`shop_id`) COMMENT '店铺id索引',
  KEY `index_order_sn` (`order_sn`) COMMENT '订单号索引',
  KEY `index_merge_sn` (`merge_sn`) COMMENT '订单号索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='订单信息表'


====================================================================================================
==========          guomiaozhishu.tb_order_detail                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_detail                               ==========
====================================================================================================
CREATE TABLE `tb_order_detail` (
  `detail_id` varchar(32) NOT NULL DEFAULT '' COMMENT '订单详情id',
  `order_id` varchar(32) DEFAULT '' COMMENT '订单ID',
  `goods_id` varchar(32) DEFAULT '' COMMENT '商品ID',
  `product_id` varchar(32) DEFAULT '' COMMENT '产品规格ID',
  `order_sn` varchar(32) DEFAULT '' COMMENT '子订单号',
  `total` int(11) DEFAULT '1' COMMENT '购买数量',
  `coment` int(1) DEFAULT '0' COMMENT '0未评价  1已评价',
  `price` decimal(10,2) DEFAULT '0.00' COMMENT '单价',
  `all_price` decimal(10,2) DEFAULT '0.00' COMMENT '总价',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`detail_id`),
  KEY `index_goods_id` (`goods_id`) COMMENT '商品id索引',
  KEY `index_product_id` (`product_id`) COMMENT '产品id索引',
  KEY `index_order_id` (`order_id`) COMMENT '订单id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='订单详情表'


====================================================================================================
==========          guomiaozhishu.tb_order_coment                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_coment                               ==========
====================================================================================================
CREATE TABLE `tb_order_coment` (
  `comment_id` varchar(32) NOT NULL DEFAULT '' COMMENT '评论表id',
  `detail_id` varchar(32) DEFAULT '' COMMENT '订单详情表id',
  `goods_id` varchar(32) DEFAULT '' COMMENT '商品id',
  `product_id` varchar(32) DEFAULT '' COMMENT '产品id',
  `member_id` varchar(32) DEFAULT '' COMMENT '评论人',
  `star` int(1) DEFAULT '5' COMMENT '评价星级 1-5,默认5星',
  `content` varchar(500) DEFAULT '' COMMENT '评价内容',
  `comment_img` varchar(500) DEFAULT '' COMMENT '评论图片',
  `disply` int(1) DEFAULT '0' COMMENT '删除标记 0：未删除，1：删除',
  `create_at` varchar(32) DEFAULT NULL COMMENT '创建时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`comment_id`),
  KEY `index_goods_id` (`goods_id`) COMMENT '商品id索引',
  KEY `index_member_id` (`member_id`) COMMENT '会员id索引',
  KEY `index_detail_id` (`detail_id`) COMMENT '详情id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='评价表'


====================================================================================================
==========          guomiaozhishu.tb_order_back                                           ==========
==========          db_ods_nstc.clife_nstc_ods_order_back                                 ==========
====================================================================================================
CREATE TABLE `tb_order_back` (
  `back_id` varchar(32) NOT NULL DEFAULT '' COMMENT '退换货id',
  `order_id` varchar(32) DEFAULT '' COMMENT '订单ID',
  `price` decimal(10,2) DEFAULT '0.00' COMMENT '退款金额',
  `express_name` varchar(64) DEFAULT '' COMMENT '物流名/司机电话',
  `express_code` varchar(64) DEFAULT '' COMMENT '快物流单号/车牌号',
  `type` int(2) DEFAULT '0' COMMENT '类型0-公司物流,1-个人找车',
  `status` int(2) DEFAULT '0' COMMENT '状态0-审核中,1-同意,2-拒绝',
  `order_status` int(2) DEFAULT '0' COMMENT '订单原状态',
  `reason` varchar(500) DEFAULT '' COMMENT '退货原因',
  `back_type` int(1) DEFAULT NULL COMMENT '0-未发货退款、1-已发货未收到货退款、2-收到货仅退款、3-收到货退货退款',
  `reason_f` varchar(500) DEFAULT '' COMMENT '驳回原因',
  `create_at` varchar(32) DEFAULT '' COMMENT '创建时间',
  `update_at` varchar(32) DEFAULT '' COMMENT '更新时间',
  `del_flag` int(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`back_id`),
  KEY `index_order_id` (`order_id`) COMMENT '订单id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='退货记录表'


====================================================================================================
==========          guomiaozhishu.tb_order_appeal                                         ==========
==========          db_ods_nstc.clife_nstc_ods_order_appeal                               ==========
====================================================================================================
CREATE TABLE `tb_order_appeal` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '申诉id',
  `order_id` varchar(32) DEFAULT '' COMMENT '订单id',
  `reason_content` varchar(255) DEFAULT '' COMMENT '申诉原因',
  `reason_img` varchar(1000) DEFAULT '' COMMENT '申诉图片',
  `status` char(2) DEFAULT '0' COMMENT '处理状态0-未处理,1-已处理',
  `create_at` varchar(32) DEFAULT '' COMMENT '申诉时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`id`),
  KEY `index_order_id` (`order_id`) COMMENT '订单id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='申诉表'


====================================================================================================
==========          guomiaozhishu.tb_member_user                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_user                                ==========
====================================================================================================
CREATE TABLE `tb_member_user` (
  `member_id` varchar(32) NOT NULL DEFAULT '' COMMENT '会员id',
  `member_phone` varchar(16) DEFAULT '' COMMENT '手机号',
  `member_name` varchar(32) DEFAULT '' COMMENT '昵称',
  `member_avator` varchar(400) DEFAULT '' COMMENT '头像',
  `password` varchar(255) DEFAULT '' COMMENT '密码',
  `token` varchar(32) DEFAULT '' COMMENT 'token',
  `status` int(2) DEFAULT '0' COMMENT '工行E企付状态0-未提交,1-工行审核中,2-审核通过,3-审核拒绝',
  `intro_user` varchar(32) DEFAULT '' COMMENT '推荐人',
  `level` int(2) DEFAULT '0' COMMENT '身份0-个人,1-企业,2-经纪人,10-未选择',
  `money` decimal(10,2) DEFAULT '0.00' COMMENT '余额',
  `freeze` decimal(10,2) DEFAULT '0.00' COMMENT '数字金额',
  `aurora_id` varchar(255) DEFAULT '' COMMENT '极光推送id',
  `create_at` datetime DEFAULT NULL COMMENT '注册时间',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `unionid` varchar(255) DEFAULT NULL COMMENT '微信unionid',
  `openid` varchar(64) DEFAULT '' COMMENT '微信openId',
  `xcxopenid` varchar(32) DEFAULT '' COMMENT '小程序openid',
  `wx_name` varchar(32) DEFAULT '' COMMENT '微信昵称',
  `ali_name` varchar(32) DEFAULT '' COMMENT '阿里名字',
  `ali_account` varchar(32) DEFAULT '' COMMENT '阿里手机号',
  `disabled` tinyint(1) DEFAULT '0' COMMENT '是否冻结账号(0-正常,1-冻结)（无用）',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  `icbc_status` int(2) DEFAULT '0' COMMENT '工行会员认证0-未认证,1-审核中,2-已认证,3-审核失败,4-提交失败',
  PRIMARY KEY (`member_id`),
  UNIQUE KEY `INDEX_MEMBER_USER_LOGINNAMAE` (`member_phone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='会员表'


====================================================================================================
==========          guomiaozhishu.tb_member_shop                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_shop                                ==========
====================================================================================================
CREATE TABLE `tb_member_shop` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员id',
  `create_at` varchar(32) DEFAULT '' COMMENT '关注时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=533 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='店铺关注表'


====================================================================================================
==========          guomiaozhishu.tb_member_question                                      ==========
==========          db_ods_nstc.clife_nstc_ods_member_question                            ==========
====================================================================================================
CREATE TABLE `tb_member_question` (
  `member_id` varchar(32) NOT NULL DEFAULT '0' COMMENT '会员id',
  `question_id` bigint(20) DEFAULT NULL COMMENT '问题id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='问题点赞表'


====================================================================================================
==========          guomiaozhishu.tb_member_follow                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_follow                              ==========
====================================================================================================
CREATE TABLE `tb_member_follow` (
  `follow_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `goods_id` varchar(32) DEFAULT '' COMMENT '商品id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员id',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`follow_id`),
  KEY `index_goods_id` (`goods_id`) COMMENT 'index_goods_id',
  KEY `index_member_id` (`member_id`) COMMENT 'index_member_id'
) ENGINE=InnoDB AUTO_INCREMENT=472 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品关注表'


====================================================================================================
==========          guomiaozhishu.tb_member_coupon                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_coupon                              ==========
====================================================================================================
CREATE TABLE `tb_member_coupon` (
  `id` varchar(32) NOT NULL DEFAULT '' COMMENT 'ID',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员ID',
  `coupon_id` varchar(32) DEFAULT '' COMMENT '优惠券ID',
  `type` int(10) DEFAULT '1' COMMENT '优惠券类型（1 满减 ）',
  `coupon_name` varchar(100) DEFAULT '' COMMENT '优惠券名称',
  `sub_money` decimal(12,2) DEFAULT '0.00' COMMENT '优惠金额',
  `coupon_money` decimal(12,0) DEFAULT '0' COMMENT '满减满足金额',
  `order_id` varchar(32) DEFAULT NULL COMMENT '订单ID',
  `status` int(10) DEFAULT '0' COMMENT '优惠券状态（0-未使用，1-已使用，2-已过期，3-已失效）',
  `create_at` varchar(32) DEFAULT '' COMMENT '获取时间',
  `order_at` varchar(32) DEFAULT '' COMMENT '使用时间',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `del_flag` int(2) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='用户优惠券'


====================================================================================================
==========          guomiaozhishu.tb_member_answer                                        ==========
==========          db_ods_nstc.clife_nstc_ods_member_answer                              ==========
====================================================================================================
CREATE TABLE `tb_member_answer` (
  `member_id` varchar(32) NOT NULL DEFAULT '0' COMMENT '会员id',
  `answer_id` bigint(20) DEFAULT NULL COMMENT '回复id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='回复点赞表'


====================================================================================================
==========          guomiaozhishu.tb_member_address                                       ==========
==========          db_ods_nstc.clife_nstc_ods_member_address                             ==========
====================================================================================================
CREATE TABLE `tb_member_address` (
  `address_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '收货地址id',
  `member_id` varchar(32) DEFAULT '' COMMENT '会员ID',
  `province` varchar(32) DEFAULT '' COMMENT '所在省',
  `city` varchar(32) DEFAULT '' COMMENT '所在市',
  `area` varchar(32) DEFAULT '' COMMENT '所在县区',
  `town` varchar(32) DEFAULT '' COMMENT '所在街道',
  `address` varchar(255) DEFAULT '' COMMENT '详细地址',
  `post_code` varchar(255) DEFAULT '' COMMENT '邮政编码',
  `full_name` varchar(255) DEFAULT '' COMMENT '收货人姓名',
  `phone` varchar(20) DEFAULT '' COMMENT '收货人电话',
  `default_value` int(2) DEFAULT '0' COMMENT '是否默认0-非默认,-1默认',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`address_id`),
  KEY `INDEX_MEMBER_USER_ADDRESS` (`member_id`)
) ENGINE=InnoDB AUTO_INCREMENT=911 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='收货地址'


====================================================================================================
==========          guomiaozhishu.tb_goods_product                                        ==========
==========          db_ods_nstc.clife_nstc_ods_goods_product                              ==========
====================================================================================================
CREATE TABLE `tb_goods_product` (
  `product_id` varchar(32) NOT NULL DEFAULT '' COMMENT '产品id',
  `batch_id` varchar(32) DEFAULT '' COMMENT '批次id',
  `sku` varchar(32) DEFAULT '' COMMENT 'SKU',
  `goods_id` varchar(32) NOT NULL DEFAULT '' COMMENT '商品ID',
  `product_name` varchar(255) DEFAULT '' COMMENT '货品名',
  `product_img` varchar(255) DEFAULT '' COMMENT '产品图片',
  `product_spec` varchar(255) DEFAULT '' COMMENT '货品规格',
  `product_price` decimal(10,2) DEFAULT '0.00' COMMENT '价格',
  `product_post` decimal(10,2) DEFAULT '0.00' COMMENT '期货购买价',
  `product_weight` int(11) DEFAULT '1' COMMENT '一包的重量',
  `product_stock` int(11) DEFAULT '0' COMMENT '库存',
  `product_buy_min` int(11) DEFAULT '1' COMMENT '期货卖出一包的重量',
  `product_buy_max` int(11) DEFAULT '0' COMMENT '期货卖出库存',
  `product_unit` varchar(25) DEFAULT '' COMMENT '计量单位',
  `product_loading` int(1) DEFAULT '0' COMMENT '是否下架(0审核1-上架2下架,3-拒绝,5-卖出)',
  `product_default` int(1) DEFAULT '0' COMMENT '是否默认(1是默认)',
  `loading_at` datetime DEFAULT NULL COMMENT '上架时间',
  `unloading__at` datetime DEFAULT NULL COMMENT '下架时间',
  `product_sale_num` int(11) DEFAULT '0' COMMENT '销售量',
  `product_location` int(11) DEFAULT '0' COMMENT '排序字段',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  `store_name` varchar(255) DEFAULT NULL COMMENT '仓库名',
  PRIMARY KEY (`product_id`),
  KEY `index_goods_id` (`goods_id`) COMMENT '商品id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品产品'


====================================================================================================
==========          guomiaozhishu.tb_goods_goods                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_goods                                ==========
====================================================================================================
CREATE TABLE `tb_goods_goods` (
  `goods_id` varchar(32) NOT NULL DEFAULT '' COMMENT '商品id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `goods_name` varchar(255) DEFAULT '' COMMENT '商品名称',
  `goods_title` varchar(500) DEFAULT '' COMMENT '商品标题',
  `class_id` varchar(32) DEFAULT '' COMMENT '商品分类',
  `type_id` varchar(32) DEFAULT '' COMMENT '商品类型',
  `goods_img` varchar(400) DEFAULT '' COMMENT '商品主图',
  `goods_note` int(3) DEFAULT '1' COMMENT '0-农资商品,1-苗木商品',
  `goods_prop` varchar(32) DEFAULT '' COMMENT '商品等级',
  `goods_spec` text COMMENT '规格详情',
  `goods_param` text COMMENT '参数详情',
  `has_spec` int(1) DEFAULT '0' COMMENT '启用规格',
  `goods_unit` varchar(25) DEFAULT '' COMMENT '计量单位',
  `goods_loading` int(1) DEFAULT '0' COMMENT '是否下架(0-待上架，1-上架,2下架，3-待审核,4-拒绝5-卖出)',
  `examine` int(2) DEFAULT '0' COMMENT '审核状态0-未审核,1-审核中,2-已通过,3-拒绝',
  `goods_reject` varchar(255) DEFAULT '' COMMENT '驳回原因',
  `goods_view_num` int(11) DEFAULT '0' COMMENT '浏览量',
  `goods_comment_num` int(11) DEFAULT '0' COMMENT '评论量',
  `goods_sale_num` int(11) DEFAULT '0' COMMENT '销售量',
  `goods_location` int(11) DEFAULT '0' COMMENT '排序字段',
  `create_id` varchar(32) DEFAULT NULL COMMENT '创建用户id',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` int(1) DEFAULT '0' COMMENT '删除标记0-显示,1-删除',
  `recom_sy` int(1) DEFAULT '0' COMMENT '0-优品优选,1-乡村振兴',
  `recommend` char(2) DEFAULT '0' COMMENT '商品推荐0-不推荐,1-推荐',
  `recom_xq` int(1) DEFAULT '0' COMMENT '邮费,0-包邮,1-不包邮',
  `recom_gwc` int(1) DEFAULT '0' COMMENT '挂牌0-普通,1-挂牌',
  `brand_id` varchar(32) DEFAULT '' COMMENT '商品品牌',
  `unloading_at` datetime DEFAULT NULL COMMENT '下架时间',
  `loading_at` datetime DEFAULT NULL COMMENT '上架时间',
  PRIMARY KEY (`goods_id`),
  KEY `index_shop_id` (`shop_id`) COMMENT 'shopId索引',
  KEY `index_class_id` (`class_id`) COMMENT '分类id索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品信息表'


====================================================================================================
==========          guomiaozhishu.tb_goods_class                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_class                                ==========
====================================================================================================
CREATE TABLE `tb_goods_class` (
  `class_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '商品分类id',
  `parent_id` bigint(20) DEFAULT '0' COMMENT '父级ID',
  `class_name` varchar(100) DEFAULT '' COMMENT '分类名称',
  `class_img` varchar(255) DEFAULT '' COMMENT '图片路径',
  `type_id` char(2) DEFAULT '0' COMMENT '类型0-苗木分类,1-农资分类',
  `class_level` tinyint(3) DEFAULT '0' COMMENT '类型等级(0-大类,1-小类)',
  `class_show` tinyint(3) DEFAULT '0' COMMENT '是否显示(0-显示,1-隐藏)',
  `class_location` int(11) DEFAULT '0' COMMENT '排序字段',
  `create_id` varchar(32) DEFAULT '' COMMENT '创建用户id',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`class_id`)
) ENGINE=InnoDB AUTO_INCREMENT=429 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品分类'


====================================================================================================
==========          guomiaozhishu.tb_goods_brand                                          ==========
==========          db_ods_nstc.clife_nstc_ods_goods_brand                                ==========
====================================================================================================
CREATE TABLE `tb_goods_brand` (
  `brand_id` varchar(32) NOT NULL DEFAULT '' COMMENT '品牌id',
  `brand_name` varchar(32) DEFAULT '' COMMENT '品牌名称',
  `brand_url` varchar(255) DEFAULT '' COMMENT '品牌网址',
  `brand_img` varchar(255) DEFAULT '' COMMENT '图片地址',
  `brand_note` text COMMENT '品牌介绍',
  `brand_location` int(11) DEFAULT '0' COMMENT '排序字段',
  `create_id` varchar(32) DEFAULT '' COMMENT '创建用户id',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` tinyint(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`brand_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品品牌'


====================================================================================================
==========          guomiaozhishu.tb_member_icbc                                          ==========
==========          db_ods_nstc.clife_nstc_ods_member_icbc                                ==========
====================================================================================================
CREATE TABLE `tb_member_icbc` (
  `icbc_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '工行认证id',
  `member_id` varchar(32) DEFAULT NULL COMMENT '会员id',
  `vendor_short_name` varchar(50) DEFAULT '' COMMENT '企业用户简称',
  `vendor_name` varchar(60) DEFAULT '' COMMENT '企业用户名称，开通 E企付担保支付时必 输',
  `vendor_phone` varchar(22) DEFAULT '' COMMENT '企业用户联系电话(会员手机号,带过来不可修改)',
  `vendor_email` varchar(50) DEFAULT '' COMMENT '企业用户邮箱',
  `province` varchar(10) DEFAULT '' COMMENT '所在省 code例130000',
  `city` varchar(10) DEFAULT '' COMMENT '所在市 code例130100',
  `county` varchar(10) DEFAULT '' COMMENT '所在区县code例130102',
  `address` varchar(60) DEFAULT '' COMMENT '详细地址',
  `postcode` varchar(6) DEFAULT '' COMMENT '邮政编码',
  `vendor_type` varchar(2) DEFAULT '' COMMENT '企业用户类型，开通 E企付担保支付时必 输 01-企业 06-个体工商户',
  `cert_type` varchar(3) DEFAULT '' COMMENT '企业用户注册证件类 型，100-全国组织机构代码 证书101-营业执照 102-行政机关 103-社会团体法人登记 证书104-军队单位开户核准 通知书 105-武警部队单位开户 核准通知书 106-下属机构(具有主管 单位批文号) 107-其他(包含统一社会 信用代码) 108-商业登记证 109-公司注册证',
  `cert_no` varchar(50) DEFAULT '' COMMENT '企业用户注册证件号码，',
  `cert_pic` varchar(255) DEFAULT '' COMMENT '企业用户注册证件图片',
  `cert_validityl_str` varchar(10) DEFAULT '' COMMENT '企业用户注册证件有 效期',
  `operator_name` varchar(60) DEFAULT '' COMMENT '企业用户联系人姓名',
  `operator_mobile` varchar(11) DEFAULT '' COMMENT '企业用户联系人手机',
  `operator_email` varchar(50) DEFAULT '' COMMENT '企业用户联系人邮箱',
  `operatorId_no` varchar(18) DEFAULT '' COMMENT '企业用户联系人身份 证号',
  `status` int(2) DEFAULT '0' COMMENT '状态0-未提交,1-审核中,2-审核成功,3-审核失败,4-提交失败',
  `reason` varchar(255) DEFAULT '' COMMENT '失败原因',
  PRIMARY KEY (`icbc_id`),
  KEY `index_member_id` (`member_id`) COMMENT '索引会员id'
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COMMENT='工行支付认证'


====================================================================================================
==========          guomiaozhishu.tb_apply_purchase                                       ==========
==========          db_ods_nstc.clife_nstc_ods_apply_purchase                             ==========
====================================================================================================
CREATE TABLE `tb_apply_purchase` (
  `purchase_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '采购id',
  `member_id` varchar(100) DEFAULT '' COMMENT '会员id',
  `purchase_num` int(10) DEFAULT '0' COMMENT '采购数量',
  `purchase_price` decimal(10,2) DEFAULT '0.00' COMMENT '价格',
  `purchase_class` varchar(32) DEFAULT '' COMMENT '采购品种',
  `purchase_level` varchar(32) DEFAULT '' COMMENT '(弃用)',
  `address` varchar(255) DEFAULT NULL COMMENT '收货地址',
  `fromword` varchar(255) DEFAULT NULL COMMENT '期望货源地',
  `go` varchar(255) DEFAULT NULL COMMENT '收获地',
  `create_at` datetime DEFAULT NULL,
  `create_by` varchar(255) DEFAULT NULL COMMENT '创建者',
  `update_at` datetime DEFAULT NULL,
  `conent` varchar(255) DEFAULT NULL COMMENT '内容(选填）',
  `image` varchar(1000) DEFAULT NULL COMMENT '图片地址',
  `del_flag` varchar(2) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL COMMENT '标题',
  `memeber_level` int(8) DEFAULT NULL COMMENT '会员身份',
  `memeber_avator` varchar(300) DEFAULT '' COMMENT '头像',
  `member_phone` varchar(16) DEFAULT NULL COMMENT '手机号',
  `statu` varchar(2) DEFAULT NULL COMMENT '0发布中 1已结束 2被驳回',
  `scannum` int(11) DEFAULT '0' COMMENT '浏览次数',
  PRIMARY KEY (`purchase_id`)
) ENGINE=InnoDB AUTO_INCREMENT=344671 DEFAULT CHARSET=utf8mb4 COMMENT='采购信息'


====================================================================================================
==========          guomiaozhishu.tb_apply_supply                                         ==========
==========          db_ods_nstc.clife_nstc_ods_apply_supply                               ==========
====================================================================================================
CREATE TABLE `tb_apply_supply` (
  `suppery_Id` bigint(11) NOT NULL AUTO_INCREMENT,
  `member_id` varchar(100) DEFAULT NULL,
  `goodclass` varchar(60) DEFAULT NULL COMMENT '品类',
  `supper_price` varchar(65) DEFAULT NULL COMMENT '供货价格',
  `level` varchar(255) DEFAULT NULL COMMENT '商品等级',
  `fromword` varchar(255) DEFAULT NULL COMMENT '货源地',
  `go` varchar(255) DEFAULT NULL COMMENT '收获地',
  `image` varchar(1000) DEFAULT NULL COMMENT '图片',
  `conment` varchar(255) DEFAULT NULL COMMENT '内容',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `createby` varchar(255) DEFAULT NULL COMMENT '创建者',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `del_flag` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL COMMENT '标题',
  `memeber_level` int(255) DEFAULT NULL COMMENT '会员身份',
  `memeber_avator` varchar(255) DEFAULT NULL COMMENT '头像',
  `member_phone` varchar(255) DEFAULT NULL COMMENT '手机号',
  `statu` varchar(255) DEFAULT NULL COMMENT '0采购中   1已结束  2被驳回',
  `scannum` int(11) DEFAULT '0' COMMENT '浏览次数',
  `supperNum` int(11) DEFAULT NULL COMMENT '供货数量',
  PRIMARY KEY (`suppery_Id`)
) ENGINE=InnoDB AUTO_INCREMENT=3524 DEFAULT CHARSET=utf8mb4 COMMENT='供应信息'


====================================================================================================
==========          guomiaozhishu.tb_storage_lease                                        ==========
==========          db_ods_nstc.clife_nstc_ods_storage_lease                              ==========
====================================================================================================
CREATE TABLE `tb_storage_lease` (
  `lease_id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT '用车id',
  `member_id` varchar(100) DEFAULT NULL COMMENT '会员id',
  `title` varchar(255) DEFAULT NULL COMMENT '标题',
  `price` decimal(10,2) DEFAULT '0.00' COMMENT '价格',
  `local_no` varchar(11) DEFAULT NULL COMMENT '冷库库位',
  `stcity` varchar(255) DEFAULT NULL COMMENT '起运城市',
  `address` varchar(255) DEFAULT NULL COMMENT '起运地详细地址',
  `encity` varchar(255) DEFAULT NULL COMMENT '目的城市',
  `area` varchar(255) DEFAULT NULL COMMENT '目的地地址',
  `content` varchar(255) DEFAULT NULL COMMENT '备注',
  `image` varchar(1000) DEFAULT NULL COMMENT '图片',
  `create_at` datetime DEFAULT NULL COMMENT '发布时间',
  `create_by` varchar(255) DEFAULT NULL COMMENT '创建人',
  `memeber_level` int(11) DEFAULT NULL COMMENT '会员身份',
  `memeber_avator` varchar(300) DEFAULT NULL COMMENT '头像',
  `member_phone` varchar(255) DEFAULT NULL COMMENT '手机号',
  `statu` varchar(2) DEFAULT NULL COMMENT '0采购中   1已结束  2被驳回',
  `scannum` int(11) DEFAULT '0' COMMENT '浏览次数',
  `longitude` varchar(50) DEFAULT '' COMMENT '经度',
  `latitude` varchar(50) DEFAULT '' COMMENT '纬度',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `del_flag` varchar(2) DEFAULT NULL COMMENT '删除标记',
  PRIMARY KEY (`lease_id`)
) ENGINE=InnoDB AUTO_INCREMENT=128 DEFAULT CHARSET=utf8mb4 COMMENT='我要用车'


====================================================================================================
==========          guomiaozhishu.tb_storage_rent                                         ==========
==========          db_ods_nstc.clife_nstc_ods_storage_rent                               ==========
====================================================================================================
CREATE TABLE `tb_storage_rent` (
  `rent_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '出租拉货id',
  `title` varchar(255) DEFAULT NULL COMMENT '标题',
  `price` decimal(10,2) DEFAULT '0.00' COMMENT '价格',
  `area` varchar(255) DEFAULT NULL COMMENT '货车类型',
  `stcity` varchar(128) DEFAULT NULL COMMENT '起运城市',
  `staddress` varchar(255) DEFAULT '' COMMENT '起运地址',
  `encity` varchar(128) DEFAULT NULL COMMENT '目的城市',
  `address` varchar(110) DEFAULT NULL COMMENT '目的地范围',
  `conent` varchar(255) DEFAULT NULL COMMENT '备注内容',
  `image` varchar(1000) DEFAULT NULL COMMENT '图片',
  `create_at` datetime DEFAULT NULL COMMENT '发布时间',
  `create_by` varchar(255) DEFAULT NULL COMMENT '创造者',
  `member_id` varchar(32) DEFAULT NULL COMMENT '会员id',
  `memeber_level` int(11) DEFAULT NULL COMMENT '会员身份',
  `memeber_avator` varchar(300) DEFAULT '' COMMENT '头像',
  `member_phone` varchar(255) DEFAULT NULL COMMENT '手机号',
  `statu` varchar(2) DEFAULT '0' COMMENT '0采购中   1已结束  2被驳回',
  `scannum` int(11) DEFAULT '0' COMMENT '浏览量',
  `longitude` varchar(50) DEFAULT '' COMMENT '经度',
  `latitude` varchar(50) DEFAULT '' COMMENT '纬度',
  `localNo` varchar(11) DEFAULT NULL COMMENT '库位',
  `update_at` datetime DEFAULT NULL COMMENT '更新时间',
  `del_flag` varchar(255) DEFAULT NULL COMMENT '删除标记',
  PRIMARY KEY (`rent_id`)
) ENGINE=InnoDB AUTO_INCREMENT=88 DEFAULT CHARSET=utf8mb4 COMMENT='我要拉货'


====================================================================================================
==========          guomiaozhishu.tb_goods_type                                           ==========
==========          db_ods_nstc.clife_nstc_ods_goods_type                                 ==========
====================================================================================================
CREATE TABLE `tb_goods_type` (
  `type_id` varchar(32) NOT NULL DEFAULT '' COMMENT '类型id',
  `shop_id` varchar(32) DEFAULT '' COMMENT '店铺id',
  `type_name` varchar(100) DEFAULT '' COMMENT '类型名称',
  `is_physical` int(1) DEFAULT '0' COMMENT '实物商品0-非实物,1-实物',
  `has_spec` int(1) DEFAULT '0' COMMENT '使用规格0-不使用,1-使用',
  `has_param` int(1) DEFAULT '0' COMMENT '使用参数0-不使用,1-使用',
  `has_brand` int(1) DEFAULT '0' COMMENT '关联品牌0-不关联,1-关联',
  `create_id` varchar(32) DEFAULT '' COMMENT '创建用户ID',
  `create_at` datetime DEFAULT NULL COMMENT '创建时间',
  `update_at` datetime DEFAULT NULL COMMENT '修改时间',
  `del_flag` int(1) DEFAULT '0' COMMENT '删除标记',
  PRIMARY KEY (`type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='商品类型'


