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