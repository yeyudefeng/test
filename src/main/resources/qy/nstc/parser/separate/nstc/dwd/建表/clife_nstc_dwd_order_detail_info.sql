create table if not exists db_dw_nstc.clife_nstc_dwd_order_detail_info ( 
     detail_id                      string               comment '订单详情id' 
    ,order_id                       string               comment '订单ID' 
    ,product_id                     string               comment '产品规格ID' 
    ,member_id                      string               comment '买家ID' 
    ,goods_id                       string               comment '商品ID' 
    ,goods_name                     string               comment '商品名称' 
    ,goods_title                    string               comment '商品标题' 
    ,class_id                       string               comment '商品分类' 
    ,class_name                     string               comment '商品分类名称' 
    ,type_id                        string               comment '商品类型' 
    ,type_name                      string               comment '商品类型名称' 
    ,brand_id                       string               comment '商品品牌' 
    ,brand_name                     string               comment '品牌名称' 
    ,brand_url                      string               comment '品牌网址' 
    ,shop_id                        string               comment '店铺id' 
    ,shop_name                      string               comment '店铺名称' 
    ,recom_xq                       int                  comment '邮费' 
    ,sku                            string               comment 'SKU' 
    ,product_name                   string               comment '货品名' 
    ,product_img                    string               comment '产品图片' 
    ,product_spec                   string               comment '货品规格' 
    ,product_price                  double               comment '价格' 
    ,product_post                   double               comment '期货购买价' 
    ,total                          int                  comment '购买数量' 
    ,is_back                        int                  comment '是否退货：1 退货，0 未退货' 
    ,shipping_id                    string               comment '订单收货信息id' 
    ,receiver_province              string               comment '省份' 
    ,receiver_city                  string               comment '城市' 
    ,receiver_area                  string               comment '区县' 
    ,receiver_town                  string               comment '街道' 
    ,create_at                      string               comment '创建时间' 
) comment '产品交易明细事实表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;