insert overwrite table db_dw_nstc.clife_nstc_dwd_deal_message_info partition(part_date) 
select 
 a.id                                             -- '消息发布id'       
,a.title                                          -- '标题'
,a.member_id                                      -- '会员id'
,b.member_name                                    -- '昵称' 
,a.goods_class                                    -- '种类'
,a.goods_num                                      -- '买卖数量'
,a.price                                          -- '交易价格'
,a.start_place                                    -- '开始地'
,a.end_place                                      -- '目的地'
,a.image                                          -- '图片'
,a.comment                                        -- '备注内容'
,a.status                                         -- '发布状态' 
,a.scannum                                        -- '浏览量'
,a.action_type                                    -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
,a.create_at                                      -- '创建时间'
,a.update_at                                      -- '更新时间'
,a.part_date                                      -- '分区日期'
from (
    select 
         purchase_id                as id               -- '消息发布id'       
        ,null                       as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,purchase_class             as goods_class      -- '种类'
        ,purchase_num               as goods_num        -- '买卖数量'
        ,purchase_price             as price            -- '交易价格'
        ,fromword                   as start_place      -- '开始地'
        ,go                         as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,conent                     as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,0                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_apply_purchase
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         suppery_Id                 as id               -- '消息发布id'       
        ,null                       as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,goodclass                  as goods_class      -- '种类'
        ,supperNum                  as goods_num        -- '买卖数量'
        ,cast(supper_price as double) as price            -- '交易价格'
        ,fromword                   as start_place      -- '开始地'
        ,go                         as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,conment                    as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,1                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_apply_supply
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         rent_id                    as id               -- '消息发布id'       
        ,title                      as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,null                       as goods_class      -- '种类'
        ,null                       as goods_num        -- '买卖数量'
        ,price                      as price            -- '交易价格'
        ,stcity                     as start_place      -- '开始地'
        ,encity                     as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,null                       as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,2                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
        from db_ods_nstc.clife_nstc_ods_storage_rent
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
    union all
    select 
         lease_id                   as id               -- '消息发布id'       
        ,title                      as title            -- '标题'
        ,member_id                  as member_id        -- '会员id'
        ,null                       as goods_class      -- '种类'
        ,null                       as goods_num        -- '买卖数量'
        ,price                      as price            -- '交易价格'
        ,stcity                     as start_place      -- '开始地'
        ,encity                     as end_place        -- '目的地'
        ,image                      as image            -- '图片'
        ,null                       as comment          -- '备注内容'
        ,statu                      as status           -- '发布状态' 
        ,scannum                    as scannum          -- '浏览量'
        ,3                          as action_type      -- '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）'
        ,create_at                  as create_at        -- '创建时间'
        ,update_at                  as update_at        -- '更新时间'
        ,part_date                                      -- '分区日期'
    from db_ods_nstc.clife_nstc_ods_storage_lease
    where part_date = regexp_replace(date_sub(current_date(),1),'-','') and del_flag = 0 
) a 
left join db_dw_nstc.clife_nstc_dim_member_user b
on a.part_date = b.part_date and a.member_id = b.member_id



