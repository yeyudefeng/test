create table if not exists db_dw_nstc.clife_nstc_dwd_deal_message_info ( 
     id                             bigint               comment '消息发布id' 
    ,title                          string               comment '标题' 
    ,member_id                      string               comment '会员id' 
    ,member_name                    string               comment '昵称' 
    ,goods_class                    string               comment '种类' 
    ,goods_num                      bigint               comment '买卖数量' 
    ,price                          double               comment '交易价格' 
    ,start_place                    string               comment '开始地' 
    ,end_place                      string               comment '目的地' 
    ,image                          string               comment '图片' 
    ,`comment`                      string               comment '备注内容' 
    ,`status`                       string               comment '发布状态' 
    ,scannum                        bigint               comment '浏览量' 
    ,action_type                    int                  comment '消息类型（0 采购，1 供应，2 车找苗，3 苗找车）' 
    ,create_at                      string               comment '创建时间' 
    ,update_at                      string               comment '更新时间' 
) comment '买卖信息发布明细事实表dwd' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;