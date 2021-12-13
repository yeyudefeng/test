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
