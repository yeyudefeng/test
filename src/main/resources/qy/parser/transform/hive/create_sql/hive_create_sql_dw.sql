====================================================================================================
==========          db_dw_hr.clife_hr_dws_market_by_product_id_d                          ==========
====================================================================================================
create table if not exists db_dw_hr.clife_hr_dws_market_by_product_id_d ( 
     product_id                     bigint               comment '产品id' 
    ,sub_org_id                     bigint               comment '养老机构id' 
    ,sub_org_name                   string               comment '养老机构名称' 
    ,org_id                         bigint               comment '养老集团id' 
    ,org_name                       string               comment '养老集团名称' 
    ,area_id                        int                  comment '区域id' 
    ,area_name                      string               comment '区域名称' 
    ,membership_amount              double               comment '签约费' 
    ,product_sign_cnt               int                  comment '签约会籍卡销售数量' 
    ,sign_date                      string               comment '签约日期' 
    ,contract_time_1_num_day        int                  comment '合同有效期1年' 
    ,contract_time_2_num_day        int                  comment '合同有效期2年' 
    ,contract_time_3_num_day        int                  comment '合同有效期3年' 
    ,contract_time_4_num_day        int                  comment '合同有效期4年' 
    ,contract_time_5_num_day        int                  comment '合同有效期5年' 
    ,contract_time_5_more_num_day   int                  comment '合同有效期大于5年' 
) comment '华润养老机构签约会籍产品累计汇总日表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


====================================================================================================
==========          db_dw_hr.clife_hr_dws_market_by_product_id_y                          ==========
====================================================================================================
create table if not exists db_dw_hr.clife_hr_dws_market_by_product_id_y ( 
     product_id                     bigint               comment '产品id' 
    ,sub_org_id                     bigint               comment '养老机构id' 
    ,sub_org_name                   string               comment '养老机构名称' 
    ,org_id                         bigint               comment '养老集团id' 
    ,org_name                       string               comment '养老集团名称' 
    ,area_id                        int                  comment '区域id' 
    ,area_name                      string               comment '区域名称' 
    ,membership_amount              double               comment '签约费' 
    ,product_sign_cnt               int                  comment '签约会籍卡销售数量' 
    ,sign_year                      string               comment '签约年份' 
    ,contract_time_1_num_year       int                  comment '合同有效期1年' 
    ,contract_time_2_num_year       int                  comment '合同有效期2年' 
    ,contract_time_3_num_year       int                  comment '合同有效期3年' 
    ,contract_time_4_num_year       int                  comment '合同有效期4年' 
    ,contract_time_5_num_year       int                  comment '合同有效期5年' 
    ,contract_time_5_more_num_year  int                  comment '合同有效期大于5年' 
) comment '华润养老机构签约会籍产品累计汇总年表' 
partitioned by (  part_date string  ) 
row format delimited fields terminated by '\t' 
stored as parquet 
;


