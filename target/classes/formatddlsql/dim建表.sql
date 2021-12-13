create table if not exists dwh_dw_campus.clife_campus_dim_area(
     area_id                         bigint               comment '县级市、区标识'
    ,area_name                       string               comment '县级市、区名称'
    ,city_id                         bigint               comment '城市标识'
    ,baidu_city_code                 int                  comment '百度城市编码'
    ,city_name                       string               comment '城市名称'
    ,city_english_name               string               comment '城市英文名称'
    ,city_longitude                  double               comment '城市经度'
    ,city_latitude                   double               comment '城市纬度'
    ,is_capital                      int                  comment '是否为省会城市（0-否 1-是）'
    ,province_id                     bigint               comment '省份标识'
    ,province_name                   string               comment '省份名称'
    ,province_english_name           string               comment '省份英文名'
    ,province_longitude              double               comment '省份经度'
    ,province_latitude               double               comment '省份纬度'
    ,baidu_code                      int                  comment '百度编码'
    ,partition_id                    bigint               comment '地理分区标识'
    ,partition_name                  string               comment '地理分区名称'
    ,country_id                      bigint               comment '国家标识'
    ,country_name                    string               comment '国家名称'
    ,country_en_name                 string               comment '国家英文名'
)comment '区域维表'
row format delimited fields terminated by '\t'
stored as parquet
;



====================
create table if not exists dwh_dw_campus.clife_campus_dim_org(
     org_id                          bigint               comment '机构id'
    ,org_name                        string               comment '机构名称'
    ,domain_name                     string               comment '机构专属域名'
    ,admin_user_id                   bigint               comment '机构管理员的用户id'
    ,parent_id                       bigint               comment '上级机构id,0表示没有上级机构'
    ,org_type                        int                  comment '机构类型( 1:教育局 2:教育集团 3:幼儿园)'
    ,grade_ids                       string               comment '幼儿园开设年级,以英文逗号隔开'
    ,org_code                        string               comment '机构代码'
    ,status                          int                  comment '状态(-1:未开始 0:停用 1：正常 2：过期)'
    ,deleted                         int                  comment '删除状态 0已删除 1正常'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
    ,start_date                      date                 comment '使用期限的开始时间'
    ,end_date                        date                 comment '使用期限的结束时间'
    ,logo                            string               comment 'logo图标路径'
    ,province_id                     int                  comment '省份id'
    ,city_id                         bigint               comment '城市id'
    ,area_id                         bigint               comment '区县id'
    ,province_city_area              string               comment '省市区'
    ,address                         string               comment '详细地址'
    ,fixed_tel_area_num              string               comment '固定电话-区号'
    ,fixed_tel_num                   string               comment '固定电话号码'
    ,phone_num                       string               comment '联系手机'
)comment '学校维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;




===================
create table if not exists dwh_dw_campus.clife_campus_dim_class(
     class_id                        bigint               comment '班级id'
    ,class_no                        string               comment '班级编号'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        int                  comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,class_room_id                   bigint               comment '教室id'
    ,status                          int                  comment '班级状态（0-失效 1-正常 2-毕业）'
    ,enter_school_year               string               comment '入学年份'
    ,leave_date                      date                 comment '毕业离园日期'
    ,graduation_time                 datetime             comment '毕业处理时间'
    ,create_time                     datetime             comment '添加时间'
    ,update_time                     datetime             comment '修改时间'
)comment '班级维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

=====================
create table if not exists dwh_dw_campus.clife_campus_dim_grade(
     grade_id                        string               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,grade_order                     int                  comment '年级排序'
    ,create_time                     datetime             comment '添加时间'
    ,update_time                     datetime             comment '修改时间'
)comment '年级维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


======================
create table if not exists dwh_dw_campus.clife_campus_dim_student(
     student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,parent_name                     string               comment '家长姓名'
    ,student_no                      string               comment '学号'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,birthday                        date                 comment '出生日期'
    ,age                             int                  comment '年龄'
    ,`month`                         int                  comment '月份'
    ,card_no                         string               comment '身份证号码'
    ,home_address                    string               comment '家庭地址'
    ,enter_school_date               date                 comment '入学日期'
    ,status                          int                  comment '学生状态（0-停用 1-正常 2-毕业 3-结业）'
    ,deleted                         int                  comment '删除 0-已删除 1-正常'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '修改时间'
    ,card_type                       int                  comment '证件类型'
    ,nation                          int                  comment '民族'
    ,avatar                          string               comment '头像'
    ,`user_id`                       bigint               comment '关联家长用户id'
    ,province_id                     int                  comment '省份id'
    ,city_id                         bigint               comment '城市id'
    ,area_id                         bigint               comment '区县id'
    ,province_city_area              string               comment '省市区'
)comment '学生维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


======================================
create table if not exists dwh_dw_campus.clife_campus_dim_interest_map_location(
     map_location_id                 bigint               comment '活动位置编号'
    ,map_id                          int                  comment '地图编号'
    ,org_id                          bigint               comment '机构id'
    ,coordinates                     string               comment '所在地图的位置坐标'
    ,location_name                   string               comment '活动位置名称'
    ,`function`                      string               comment '位置使用功能'
    ,status                          int                  comment '状态'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
    ,`type`                          int                  comment '类型'
    ,class_room_id                   bigint               comment '教室唯一标识'
    ,class_id                        bigint               comment '班级唯一标识'
    ,number_people                   int                  comment '限定人数'
    ,areas_id                        bigint               comment '公共区域id'
)comment '活动室维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

=========================================
create table if not exists  dwh_dw_campus.clife_campus_dim_disposition_tag(
     tag_id                          int                  comment '标签id,自增'
    ,tag_name                        string               comment '标签名'
    ,mood_type                       int                  comment '人格id'
    ,disposition_type                int                  comment '特性id'
    ,is_negative                     tinyint              comment '是否负面 0-不是 1-是 (冗余字段)'
    ,score                           int                  comment '标签分值'
    ,status                          tinyint              comment '状态 0-无效 1-草稿 2-正式'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '性格画像的标签表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

=========================================================



create table if not exists dwh_dw_campus.clife_campus_dim_sleep_status(
     status_id                       int                  comment '主键'
    ,status_desc                     string               comment '状态描述'
)comment '睡眠状态表'
row format delimited fields terminated by '\t'
stored as parquet
;



create table if not exists dwh_dw_campus.clife_campus_dim_body_source(
     source_id                       int                  comment '主键'
    ,source_desc                     string               comment '来源描述'
)comment '身高体重来源表'
row format delimited fields terminated by '\t'
stored as parquet
;



create table if not exists dwh_dw_campus.clife_campus_dim_bmi_type(
     type_id                         int                  comment '主键'
    ,type_desc                       string               comment '类型描述'
)comment 'bmi类型表'
row format delimited fields terminated by '\t'
stored as parquet
;



create table if not exists dwh_dw_campus.clife_campus_dim_attendance_status(
     status_id                       int                  comment '主键'
    ,status_desc                     string               comment '状态描述'
)comment '入园状态表'
row format delimited fields terminated by '\t'
stored as parquet
;


============================
create table clife_campus_dim_ingredients_info(
     ingredient_id                   int                  comment '编号'
    ,ingredient_name                 string               comment '食材名称'
    ,total_category_id               int                  comment '大分类id'
    ,first_category_id               int                  comment '一级分类id'
    ,second_category_id              int                  comment '二级分类id'
    ,description                     string               comment '营养描述'
    ,edible_part                     double               comment '可食部（%）'
    ,water_content                   double               comment '水分'
    ,energy                          double               comment '能量(kcal)'
    ,protein                         double               comment '蛋白质(g)'
    ,fat                             double               comment '脂肪(g)'
    ,carbohydrate                    double               comment '碳水化合物(g)'
    ,insoluble_fiber                 double               comment '不溶性纤维素(g)'
    ,total_vitamina                  double               comment '总维生素a(ugrae)'
    ,vitaminb1                       double               comment '维生素b1(mg)'
    ,vitaminb2                       double               comment '维生素b2(mg)'
    ,nick_acid                       double               comment '尼克酸(mg)'
    ,vitaminc                        double               comment '维生素c(mg)'
    ,calcium                         double               comment '钙(mg)'
    ,iron                            double               comment '铁(mg)'
    ,zinc                            double               comment '锌(mg)'
    ,status                          tinyint              comment '状态：0-无效；1-有效'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '修改时间'
    ,is_recommend                    tinyint              comment '是否推荐（1-是；2-否）'
    ,is_allergy                      tinyint              comment '1过敏食材0不是'
    ,ingredient_alias                string               comment '食材别名'
    ,url                             string               comment '食材图片url'
    ,is_common                       int                  comment '跟大数据食材库是否相交：1-相交;null-不相交'
)comment '食材列表'
partitioned by(part_date string)
;


create table if not exists dwh_dw_campus.clife_campus_dim_interest_activity(
     activity_id                     bigint               comment '活动id'
    ,activity_name                   string               comment '活动名称'
    ,start_time                      string               comment '开始时间'
    ,end_time                        string               comment '结束时间'
    ,org_id                          bigint               comment '学校id'
    ,class_id                        bigint               comment '班级id'
    ,week                            string               comment '日期'
    ,status                          tinyint              comment '状态'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
)comment '活动表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


====================
create table if not exists dwh_dw_campus.clife_campus_dim_water_drink_setting(
     data_id                         bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,grade_id                        int                  comment '年级'
    ,enough                          int                  comment '饮水量充足'
    ,standard                        int                  comment '饮水量达标'
    ,device_type                     tinyint              comment '饮水设备类型：1-净饮机，2-刷卡器'
    ,detail                          tinyint              comment '明细设置（0-家长不可见，1-家长可见）'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '修改时间'
    ,deleted                         tinyint              comment '（是否删除）0-删除，1-正常'
)comment '饮水配置维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;
====================
create table if not exists dwh_dw_campus.clife_campus_dim_sports_assess_setting(
     data_id                         bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,grade_id                        int                  comment '年级'
    ,adequate                        int                  comment '充足'
    ,standard                        int                  comment '达标'
    ,insufficient                    int                  comment '不足'
    ,create_time                     datetime             comment '修改时间'
    ,update_time                     datetime             comment '创建时间'
)comment '运动配置维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

==================
create table clife_campus_dim_system_dict_item (
     dict_id                         int                  comment '字典id'
    ,item_text                       string               comment '字典项文本'
    ,item_value                      string               comment '字典项值'
    ,description                     string               comment '描述'
    ,show_order                      int                  comment '排序'
    ,deleted                         tinyint              comment '0：删除，1：正常'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '字典选项';


create table if not exists dwh_dw_campus.clife_campus_dim_into_campus_detail(
     record_id                       bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       datetime             comment '数据日期'
    ,am_status                       tinyint              comment '入园状态(0-未入园、1-已入园)'
    ,am_time                         datetime             comment '入园时间(hh:mm:ss)'
    ,`type`                          tinyint              comment '条件类型（1-考勤、2-饮水、3-步数、4-运动量、5-晨检）'
    ,create_time                     datetime             comment '创建时间'
)comment '入园维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

===================
--教师维表
create table dwh_dw_campus.clife_campus_dim_staff_archives(
     data_id                         bigint               comment '自增主键'
    ,archives_id                     bigint               comment '档案id(同用户id)'
    ,real_name                       string               comment '昵称'
    ,sex                             int                  comment '性别'
    ,age                             int                  comment '年龄'
    ,mobile                          string               comment '手机'
    ,email                           string               comment '邮箱'
    ,id_card_type                    tinyint              comment '证件类型'
    ,id_card                         string               comment '证件号码'
    ,birthday                        date                 comment '生日'
    ,avatar                          string               comment '头像'
    ,nation                          int                  comment '民族'
    ,political_status                int                  comment '政治面貌'
    ,education                       int                  comment '最高学历'
    ,education_desc                  string               comment '学历描述'
    ,class_ids                       string               comment '班级id'
    ,class_names                     string               comment '班级名称'
    ,grade_ids                       string               comment '年级id'
    ,grade_names                     string               comment '年级名称'
    ,org_id                          bigint               comment '机构id'
    ,org_name                        string               comment '学校名称'
    ,province_id                     bigint               comment '省份id'
    ,city_id                         bigint               comment '城市id'
    ,area_id                         bigint               comment '区县id'
    ,province_city_area              string               comment '省市区'
    ,address                         string               comment '详细地址'
    ,clife_id                        bigint               comment '公共平台的用户id'
    ,position_id                     bigint               comment '岗位id'
    ,position_name                   string               comment '岗位名称'
    ,dept_id                         bigint               comment '部门id'
    ,staff_type                      int                  comment '人员类型(1-教师，2-教职工)'
    ,entry_date                      datetime             comment '入职日期'
    ,status                          int                  comment '状态（0-停用 1-正常）'
    ,change_type                     int                  comment '异动类型（1-在职,2-离职,3-辞退,4-外部调转,5-退休）'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '修改时间'
)comment '教师维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


=================================================
--用户维表

create table dwh_dw_campus.clife_campus_dim_user(
     `user_id`                       bigint               comment '用户id'
    ,username                        string               comment '账号'
    ,real_name                       string               comment '昵称'
    ,sex                             tinyint              comment '性别'
    ,age                             tinyint              comment '年龄'
    ,mobile                          string               comment '手机'
    ,email                           string               comment '邮箱'
    ,id_card_type                    tinyint              comment '证件类型'
    ,id_card                         string               comment '证件号码'
    ,birthday                        date                 comment '生日'
    ,nation                          tinyint              comment '民族'
    ,political_status                tinyint              comment '政治面貌'
    ,education                       tinyint              comment '最高学历'
    ,province_id                     bigint               comment '省份id'
    ,city_id                         bigint               comment '城市id'
    ,area_id                         bigint               comment '区县id'
    ,province_city_area              string               comment '省市区'
    ,address                         string               comment '详细地址'
    ,clife_id                        bigint               comment '公共平台的用户id'
    ,status                          tinyint              comment '正常状态'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
)comment '用户基础数据表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


===================================================
--地区维表

create table dwh_dw_campus.clife_campus_dim_geographic_info(
     area_id                         bigint               comment '县级市、区id'
    ,area_name                       string               comment '县级市、区名称'
    ,city_id                         bigint               comment '城市id'
    ,baidu_city_code                 string               comment '百度城市编码'
    ,city_name                       string               comment '城市名称'
    ,city_english_name               string               comment '城市英文名称'
    ,city_longitude                  decimal(15,8)        comment '城市经度'
    ,city_latitude                   decimal(15,8)        comment '城市纬度'
    ,is_capital                      int                  comment '是否为省会城市'
    ,province_id                     int                  comment '省份id'
    ,province_name                   string               comment '省份名称'
    ,province_english_name           string               comment '省份英文名'
    ,province_longitude              decimal(15,8)        comment '省份经度'
    ,province_latitude               decimal(15,8)        comment '省份纬度'
    ,baidu_code                      int                  comment '省份百度编码'
    ,partition_id                    int                  comment '县级市、区id'
    ,partition_name                  string               comment '县级市、区名称'
    ,country_id                      int                  comment '国家id'
    ,country_name                    string               comment '国家名称'
    ,country_en_name                 string               comment '国家英文名'
)comment '地域信息表'
partitioned by (part_date string comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;


======================================================
--岗位维表
create table dwh_dw_campus.clife_campus_dim_position(
     position_id                     bigint               comment '岗位id'
    ,org_id                          bigint               comment '机构id'
    ,position_name                   string               comment '岗位名称'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '岗位维表'
partitioned by (part_date string comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;
