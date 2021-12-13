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