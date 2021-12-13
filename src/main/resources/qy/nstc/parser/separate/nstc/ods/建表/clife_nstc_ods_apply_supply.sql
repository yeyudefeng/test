create table if not exists db_ods_nstc.clife_nstc_ods_apply_supply ( 
     suppery_Id                     bigint               comment '供应id' 
    ,member_id                      string               comment '会员id' 
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
    ,del_flag                       string               comment '是否删除' 
    ,title                          string               comment '标题' 
    ,memeber_level                  int                  comment '会员身份' 
    ,memeber_avator                 string               comment '头像' 
    ,member_phone                   string               comment '手机号' 
    ,statu                          string               comment '0采购中   1已结束  2被驳回' 
    ,scannum                        int                  comment '浏览次数' 
    ,supperNum                      int                  comment '供货数量' 
) comment '供应信息' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;