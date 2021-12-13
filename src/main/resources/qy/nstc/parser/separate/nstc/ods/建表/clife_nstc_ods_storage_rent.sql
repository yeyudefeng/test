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
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;