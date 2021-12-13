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
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;