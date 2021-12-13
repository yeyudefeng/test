create table if not exists db_dw_hr.clife_hr_dws_market_by_sub_org_f(
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_first_intent_high_cnt       int                     comment   '客户跟踪模块首次意向感兴趣客户数',
pension_first_intent_mid_cnt        int                     comment   '客户跟踪模块首次意向兴趣一般客户数',
pension_first_intent_low_cnt        int                     comment   '客户跟踪模块首次意向不感兴趣客户数',
pension_first_intent_highest_cnt    int                     comment   '客户跟踪模块首次意向希望购买客户数',
pension_intent_high_cnt             int                     comment   '客户跟踪模块当前意向感兴趣客户数',
pension_intent_mid_cnt              int                     comment   '客户跟踪模块当前意向兴趣一般客户数',
pension_intent_low_cnt              int                     comment   '客户跟踪模块当前意向不感兴趣客户数',
pension_intent_highest_cnt          int                     comment   '客户跟踪模块当前意向希望购买客户数',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
card_pay_amount                     double           comment	  '刷卡金额',
cash_pay_amount                     double           comment   '现金金额',
weixin_pay_amount                   double           comment   '微信支付金额',
alipay_amount                       double           comment   '阿里支付金额',	
other_pay_amount                    double           comment   '其他方式支付金额',
received_amount                     double           comment   '已收金额',
received_installment_amount         double           comment   '已收分期金额',
received_occupancy_amount           double           comment   '已收占用费',
membership_amount                   double           comment   '会籍费',
overdue_amount                      double           comment   '滞纳金',
received_overdue_amount             double           comment   '已收滞纳金',
other_amount                        double           comment   '其他费用',
manager_amount                      double           comment   '管理费',
trust_amount                        double           comment   '托管费', 
membership_discount_amount          double           comment   '会籍优惠价',  
card_amount                         double           comment   '卡费',
card_manager_amount                 double           comment   '卡管理费',
other_transfor_to_amount            double           comment   '其他转让费' ,
transfor_to_amount                  double           comment   '转让费', 
other_transfor_from_amount          double           comment   '其他继承费' ,
transfor_from_amount                double           comment   '继承费',
withdraw_amount                     double           comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
ask_check_in_cust_cnt               int                     comment   '发起入住通知客户数',
assess_cnt                          int                     comment   '评估总次数',
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;





create table if not exists db_dw_hr.clife_hr_dws_market_by_sub_org_d(
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_first_intent_high_cnt       int                     comment   '客户跟踪模块首次意向感兴趣客户数',
pension_first_intent_mid_cnt        int                     comment   '客户跟踪模块首次意向兴趣一般客户数',
pension_first_intent_low_cnt        int                     comment   '客户跟踪模块首次意向不感兴趣客户数',
pension_first_intent_highest_cnt    int                     comment   '客户跟踪模块首次意向希望购买客户数',
pension_intent_high_cnt             int                     comment   '客户跟踪模块当前意向感兴趣客户数',
pension_intent_mid_cnt              int                     comment   '客户跟踪模块当前意向兴趣一般客户数',
pension_intent_low_cnt              int                     comment   '客户跟踪模块当前意向不感兴趣客户数',
pension_intent_highest_cnt          int                     comment   '客户跟踪模块当前意向希望购买客户数',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
card_pay_amount                     double           comment	  '刷卡金额',
cash_pay_amount                     double           comment   '现金金额',
weixin_pay_amount                   double           comment   '微信支付金额',
alipay_amount                       double           comment   '阿里支付金额',	
other_pay_amount                    double           comment   '其他方式支付金额',
received_amount                     double           comment   '已收金额',
other_transfor_to_amount            double           comment   '其他转让费' ,
transfor_to_amount                  double           comment   '转让费', 
other_transfor_from_amount          double           comment   '其他继承费' ,
transfor_from_amount                double           comment   '继承费',
withdraw_amount                     double           comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
ask_check_in_cust_cnt               int                     comment   '发起入住通知客户数',
assess_cnt                          int                     comment   '评估总次数',
data_date                           string                  comment   '数据日期'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists db_dw_hr.clife_hr_dws_market_by_channel_f(
channel_id                          string                  comment   '渠道id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
channel_name                        string                  comment   '渠道名称',
cannel_type                         int                     comment   '渠道类型',
channel_type_name                   String                  comment   '渠道类型名称',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
deposit_ratio                       double                  comment   '下定转化率',
sign_cust_cnt                       int                     comment   '签约客户数',
sign_ratio                          double                  comment   '签约转化率',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额'
)comment '渠道营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_channel_d(
channel_id                          string                  comment   '渠道id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
channel_name                        string                  comment   '渠道名称',
cannel_type                         int                     comment   '渠道类型',
channel_type_name                   String                  comment   '渠道类型名称',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_date                           string                  comment   '数据日期'
)comment '渠道营销管理日汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_channel_m(
channel_id                          string                  comment   '渠道id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
channel_name                        string                  comment   '渠道名称',
cannel_type                         int                     comment   '渠道类型',
channel_type_name                   String                  comment   '渠道类型名称',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_month                          string                  comment   '数据月份'
)comment '渠道营销管理月汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_channel_y(
channel_id                          string                  comment   '渠道id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
channel_name                        string                  comment   '渠道名称',
cannel_type                         int                     comment   '渠道类型',
channel_type_name                   String                  comment   '渠道类型名称',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_year                           string                  comment   '数据年份'
)comment '渠道营销管理年汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;






create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_f(
membership_adviser_id               bigint                  comment   '会籍顾问id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
membership_adviser_name             string                  comment   '会籍顾问名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
crm_wait_vist_cnt                   int                     comment   '营销管理待回访客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
)comment '华润会籍顾问营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_d(
membership_adviser_id               bigint                  comment   '会籍顾问id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
membership_adviser_name             string                  comment   '会籍顾问名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
data_date                           string                  comment   '数据日期'
)comment '华润会籍顾问营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_m(
membership_adviser_id               bigint                  comment   '会籍顾问id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
membership_adviser_name             string                  comment   '会籍顾问名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
data_month                          string                  comment   '数据月份'
)comment '华润会籍顾问营销管理月汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_adv_member_y(
membership_adviser_id               bigint                  comment   '会籍顾问id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
membership_adviser_name             string                  comment   '会籍顾问名称',
advisory_customer_cnt               int                     comment   '咨询客户数',
pension_intent_cnt                  int                     comment   '客户跟踪模块意向客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
advisory_cust_visit_cnt             int                     comment   '咨询客户回访次数',
pension_intent_cust_visit_cnt       int                     comment   '客户跟踪模块意向客户回访次数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_advisory_cust_visit_cnt         double                  comment   '咨询客户平均回访次数',
avg_pension_intent_cust_visit_cnt   double                  comment   '客户跟踪模块意向客户平均回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
data_year                           string                  comment   '数据年份'
)comment '华润会籍顾问营销管理年汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_building_f(
building_id                         bigint                  comment   '楼栋id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
card_pay_amount                     double                  comment	  '刷卡金额',
cash_pay_amount                     double                  comment   '现金金额',
weixin_pay_amount                   double                  comment   '微信支付金额',
alipay_amount                       double                  comment   '阿里支付金额',	
other_pay_amount                    double                  comment   '其他方式支付金额',
received_amount                     double                  comment   '已收金额',
received_installment_amount         double                  comment   '已收分期金额',
received_occupancy_amount           double                  comment   '已收占用费',
membership_amount                   double                  comment   '会籍费',
overdue_amount                      double                  comment   '滞纳金',
received_overdue_amount             double                  comment   '已收滞纳金',
other_amount                        double                  comment   '其他费用',
manager_amount                      double                  comment   '管理费',
trust_amount                        double                  comment   '托管费', 
membership_discount_amount          double                  comment   '会籍优惠价',  
card_amount                         double                  comment   '卡费',
card_manager_amount                 double                  comment   '卡管理费',
other_transfor_to_amount            double                  comment   '其他转让费' ,
transfor_to_amount                  double                  comment   '转让费', 
other_transfor_from_amount          double                  comment   '其他继承费' ,
transfor_from_amount                double                  comment   '继承费',
withdraw_amount                     double                  comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
assess_cnt                          int                     comment   '评估总次数',
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_building_d(
building_id                         bigint                  comment   '楼栋id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
card_pay_amount                     double                  comment	  '刷卡金额',
cash_pay_amount                     double                  comment   '现金金额',
weixin_pay_amount                   double                  comment   '微信支付金额',
alipay_amount                       double                  comment   '阿里支付金额',	
other_pay_amount                    double                  comment   '其他方式支付金额',
received_amount                     double                  comment   '已收金额',
other_transfor_to_amount            double                  comment   '其他转让费' ,
transfor_to_amount                  double                  comment   '转让费', 
other_transfor_from_amount          double                  comment   '其他继承费' ,
transfor_from_amount                double                  comment   '继承费',
withdraw_amount                     double                  comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
assess_cnt                          int                     comment   '评估总次数',
data_date                           string                  comment   '数据日期'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists db_dw_hr.clife_hr_dws_market_by_building_m(
building_id                         bigint                  comment   '楼栋id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
card_pay_amount                     double                  comment	  '刷卡金额',
cash_pay_amount                     double                  comment   '现金金额',
weixin_pay_amount                   double                  comment   '微信支付金额',
alipay_amount                       double                  comment   '阿里支付金额',	
other_pay_amount                    double                  comment   '其他方式支付金额',
received_amount                     double                  comment   '已收金额',
other_transfor_to_amount            double                  comment   '其他转让费' ,
transfor_to_amount                  double                  comment   '转让费', 
other_transfor_from_amount          double                  comment   '其他继承费' ,
transfor_from_amount                double                  comment   '继承费',
withdraw_amount                     double                  comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
assess_cnt                          int                     comment   '评估总次数',
date_month                          string                  comment   '数据月份'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;

create table if not exists db_dw_hr.clife_hr_dws_market_by_building_y(
building_id                         bigint                  comment   '楼栋id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
crm_intent_cust_visit_cnt           int                     comment   '营销管理模块意向客户回访次数',
deposit_cust_visit_cnt              int                     comment   '下定客户回访次数',
sign_cust_visit_cnt                 int                     comment   '签约客户回访次数',
avg_crm_intent_cust_visit_cnt       double                  comment   '营销管理模块意向客户平均回访次数',
avg_deposit_cust_visit_cnt          double                  comment   '下定客户平均回访次数',
avg_sign_cust_visit_cnt             double                  comment   '签约客户平均回访次数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double                  comment   '下定额',
sales_amount                        double                  comment   '销售额',
card_pay_amount                     double                  comment	  '刷卡金额',
cash_pay_amount                     double                  comment   '现金金额',
weixin_pay_amount                   double                  comment   '微信支付金额',
alipay_amount                       double                  comment   '阿里支付金额',	
other_pay_amount                    double                  comment   '其他方式支付金额',
received_amount                     double                  comment   '已收金额',
other_transfor_to_amount            double                  comment   '其他转让费' ,
transfor_to_amount                  double                  comment   '转让费', 
other_transfor_from_amount          double                  comment   '其他继承费' ,
transfor_from_amount                double                  comment   '继承费',
withdraw_amount                     double                  comment   '退会金额', 
assess_cust_cnt                     int                     comment   '评估客户数',
assess_cnt                          int                     comment   '评估总次数',
data_year                           string                  comment   '数据年份'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_f(
room_type                           int                     comment   '房型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_d(
room_type                           int                     comment   '房型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_date                           string                  comment   '数据日期'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_m(
room_type                           int                     comment   '房型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_month                          string                  comment   '数据月份'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists db_dw_hr.clife_hr_dws_market_by_room_type_y(
room_type                           int                     comment   '房型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_year                           string                  comment   '数据年份'
)comment '华润养老机构营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;






create table if not exists db_dw_hr.clife_hr_dws_market_by_card_f(
card_id                             bigint                  comment   '会籍卡类型id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额'
)comment '会籍卡营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_d(
card_id                             bigint                  comment   '会籍卡类型id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_date                           string                  comment   '数据日期'
)comment '会籍卡营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_m(
card_id                             bigint                  comment   '会籍卡类型id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_month                          string                  comment   '数据月份'
)comment '会籍卡营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_y(
card_id                             bigint                  comment   '会籍卡类型id',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_year                           string                  comment   '数据年份'
)comment '会籍卡营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_f(
card_type                           bigint                  comment   '会籍卡类型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额'
)comment '会籍卡类型营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_d(
card_type                           bigint                  comment   '会籍卡类型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_date                           string                  comment   '数据日期'
)comment '会籍卡类型营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_m(
card_type                           bigint                  comment   '会籍卡类型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_month                          string                  comment   '数据月份'
)comment '会籍卡类型营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists db_dw_hr.clife_hr_dws_market_by_card_type_y(
card_type                           bigint                  comment   '会籍卡类型',
sub_org_id                          bigint                  comment   '养老机构id',
sub_org_name                        string                  comment   '养老机构名称',
org_id                              bigint                  comment   '养老集团id',
org_name                            string                  comment   '养老集团名称',
area_id                             int                     comment   '区域id',
area_name                           string                  comment   '区域名称',
crm_first_intent_high_cnt           int                     comment   '营销管理模块首次意向感兴趣客户数',
crm_first_intent_mid_cnt            int                     comment   '营销管理模块首次意向兴趣一般客户数',
crm_first_intent_low_cnt            int                     comment   '营销管理模块首次意向不感兴趣客户数',
crm_first_intent_highest_cnt        int                     comment   '营销管理模块首次意向希望购买客户数',
crm_intent_high_cnt                 int                     comment   '营销管理模块当前意向感兴趣客户数',
crm_intent_mid_cnt                  int                     comment   '营销管理模块当前意向兴趣一般客户数',
crm_intent_low_cnt                  int                     comment   '营销管理模块当前意向不感兴趣客户数',
crm_intent_highest_cnt              int                     comment   '营销管理模块当前意向希望购买客户数',
crm_intent_cnt                      int                     comment   '营销管理模块意向客户数',
deposit_cust_cnt                    int                     comment   '下定客户数',
sign_cust_cnt                       int                     comment   '签约客户数',
transfer_to_member_cnt              int                     comment   '转让会籍数',
transfer_from_member_cnt            int                     comment   '继承会籍数',
withdraw_member_cnt                 int                     comment   '退会会籍数',
apply_member_cnt                    int                     comment   '预申请会籍数量',
deposit_member_cnt                  int                     comment   '下定会籍数量',
deposit_room_cnt                    int                     comment   '下定房间数量',
sign_member_cnt                     int                     comment   '签约会籍数量',
sign_room_cnt                       int                     comment   '签约房间数量',
deposit_amount                      double           comment   '下定额',
sales_amount                        double           comment   '销售额',
data_year                           string                  comment   '数据年份'
)comment '会籍卡类型营销管理累计汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;













