insert overwrite table db_dw_nstc.clife_nstc_dim_township_area partition(part_date)
select 
     id                             -- 'id' 
    ,name_prov                      -- '省' 
    ,code_prov                      -- '省code' 
    ,name_city                      -- '市' 
    ,code_city                      -- '市code' 
    ,name_coun                      -- '区' 
    ,code_coun                      -- '区code' 
    ,name_town                      -- '镇' 
    ,code_town                      -- '镇code'
    ,part_date                      -- '分区日期'
from db_ods_nstc.clife_nstc_ods_township_area
where part_date = regexp_replace(date_sub(current_date(),1),'-','') 