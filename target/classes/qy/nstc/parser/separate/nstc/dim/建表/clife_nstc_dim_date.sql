create table if not exists db_dw_nstc.clife_nstc_dim_date ( 
     dt_code                        string               comment '日期代码' 
    ,date_s                         string               comment '日期代码/' 
    ,date_w                         string               comment '日期代码-' 
    ,date_cn                        string               comment '日期代码-CN' 
    ,y_code                         string               comment '年份代码' 
    ,q_code                         string               comment '季度代码' 
    ,q_code_cn                      string               comment '季度-CN' 
    ,m_code                         string               comment '月份代码' 
    ,m_code_cn                      string               comment '月份-CN' 
    ,week_of_month_cn               string               comment '本月第几周' 
    ,week_of_year_cn                string               comment '本年第几周' 
    ,d_code                         string               comment '日' 
    ,day_of_year_cn                 string               comment '本年第几天' 
    ,week_cn                        string               comment '星期几' 
) comment '时间维表' 
row format delimited fields terminated by '\t' 
stored as parquet 
;