create table clife_campus_ods_allergy_ingredients(
     record_id                       int                  comment '记录id'
    ,student_id                      bigint               comment '学生唯一id'
    ,ingredient_id                   int                  comment '食材id'
    ,checkup_id                      int                  comment '幼儿健康-体检管理添加的过敏食材'
) comment '过敏食材表'
partitioned by(part_date string)

====================
create table clife_campus_ods_config(
     data_id                         int                  comment '唯一标识id'
    ,serial_number                   int                  comment '序号'
    ,name                            string               comment '名称'
    ,group_id                        int                  comment '分组id'
    ,group_name                      string               comment '分组名称'
) comment '一些基本配置信息表'
partitioned by(part_date string)


====================
create table clife_campus_ods_stu_health(
     data_id                         bigint               comment '唯一标识'
    ,student_data_id                 bigint               comment '学生id'
    ,history_of_convulsions          int                  comment '高热惊厥史'
    ,attack_temp                     double               comment '发作时温度'
    ,number_of_attacks               int                  comment '发作次数'
    ,serious_disease_his             int                  comment '各种严重病史'
    ,serious_disease_his_d           string               comment '严重病史详情'
    ,infectious_disease_his          int                  comment '各种传染病史'
    ,infectious_disease_his_d        string               comment '传染病史详情'
    ,other_infectious_disease_his    string               comment ' 其他传染病史'
    ,weight                          double               comment '体重（单位：kg）'
    ,weight_assess                   int                  comment '0：中上，1：中等，2：中下'
    ,height                          double               comment '身高（单位：cm）'
    ,height_assess                   int                  comment '0：中上，1：中等，2：中下'
    ,bmi                             double               comment 'bmi数值'
    ,bmi_assess                      int                  comment '0：中上，1：中等，2：中下'
    ,left_eye                        int                  comment '左眼健康'
    ,right_eye                       int                  comment '右眼健康'
    ,left_vision                     double               comment '左视力'
    ,right_vision                    double               comment '右视力'
    ,number_of_caries                int                  comment '龋齿数'
    ,number_of_teeth                 int                  comment '牙齿数量'
    ,head_neck                       int                  comment ' 头颈'
    ,thoracic                        int                  comment '胸廓'
    ,spine                           int                  comment '脊椎'
    ,limbs                           int                  comment '四肢'
    ,pharynx                         int                  comment '咽部'
    ,heart                           int                  comment '心'
    ,lung                            int                  comment '肺'
    ,liver                           int                  comment '肝'
    ,spleen                          int                  comment '脾'
    ,hemoglobin                      double               comment '血红蛋白（单位：g/h）'
    ,alt                             int                  comment '丙氨酸氨基转移酶'
    ,complete                        int                  comment '1：齐全，0：不齐'
    ,lack                            string               comment '接种情况'
    ,create_time                     datetime             comment '创建时间'
    ,whether_allergy                 int                  comment '是否过敏体质，0：否，1：是'
    ,status                          int                  comment '0：失效，1：正常，2：停用'
    ,update_time                     datetime             comment '更新时间'
    ,`resource`                      int                  comment '0:健康档案，1：健康体检'
    ,checkup_id                      int                  comment '体检id'
    ,deleted                         int                  comment '0：删除，1：正常'
) comment '健康档案表'
partitioned by(part_date string)

==================================
create table clife_campus_ods_robot_check_report(
     data_id                         int                  comment '主键标识'
    ,device_id                       bigint               comment '设备id'
    ,mac_address                     string               comment 'mac地址'
    ,product_id                      int                  comment '产品标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       string               comment '数据日期'
    ,card_no                         string               comment '卡号'
    ,measure_time                    string               comment '测量时间'
    ,temperature                     string               comment '体温'
    ,level                           tinyint              comment '体温状态'
    ,hand                            tinyint              comment '手部异常'
    ,mouth                           tinyint              comment '口腔异常'
    ,red_eye                         tinyint              comment '红眼'
    ,fever                           tinyint              comment '发烧'
    ,cheek                           tinyint              comment '腮部肿大'
    ,spirit                          tinyint              comment '精神不佳'
    ,throat                          tinyint              comment '咽炎'
    ,trauma                          tinyint              comment '外伤'
    ,cough                           tinyint              comment '咳嗽'
    ,fingernail                      tinyint              comment '指甲过长'
    ,tooth_decay                     tinyint              comment '蛀牙'
    ,other                           tinyint              comment '其他异常'
    ,hand_img                        string               comment '手部异常图像'
    ,mouth_img                       string               comment '口腔异常图像'
    ,eye_img                         string               comment '眼睛异常图像'
    ,data_type                       tinyint              comment '数据类型'
    ,abnormal_num                    tinyint              comment '异常数量'
    ,upload_time                     string               comment '上传时间'
    ,access                          string               comment '是否允许入园(y-是、n-否)'
) comment '晨检机器人晨检报告表'
partitioned by(part_date string)

==========================
create table clife_campus_ods_vision_data(
     vision_id                       bigint               comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,left_vision                     double               comment '左眼视力'
    ,right_vision                    double               comment '右眼视力'
    ,left_abnormal                   tinyint              comment '左眼情况'
    ,right_abnormal                  tinyint              comment '右眼情况'
    ,source                          tinyint              comment '数据来源'
    ,measure_time                    string               comment '测量日期'
    ,upload_time                     string               comment '上传时间'
) comment '智能校园学生视力表'
partitioned by(part_date string)






===========
create table clife_campus_ods_sleep_day_data(
     data_id                         bigint               comment '主键'
    ,device_no                       string               comment '设备编号'
    ,student_id                      bigint               comment '学生id'
    ,`date`                          date                 comment '数据所在日期'
    ,sleep_duration                  int                  comment '睡眠时长（秒）'
    ,wake_duration                   int                  comment '清醒时长（秒）'
    ,wake_number                     int                  comment '清醒次数'
    ,score                           double               comment '睡眠打分'
    ,source                          int                  comment '数据来源(1-天波手环，3-睡眠带子)'
    ,upload_time                     timestamp            comment '上传时间(utc)'
    ,create_time                     timestamp            comment '创建时间'
) comment '学生日睡眠报告表'
partitioned by(part_date string)

create table clife_campus_ods_sleep_day_status(
     device_no                       string               comment '设备id'
    ,student_id                      bigint               comment '学生id'
    ,date                            date                 comment '数据所在日期'
    ,start_time                      timestamp            comment '睡眠状态开始时间(utc)'
    ,status                          tinyint              comment '睡眠状态（1-上床，2-入睡，3-浅睡，4-深睡，5-清醒，6-睡着，7-离床，8-再次上床，9-中途起床，10-微觉醒）'
    ,source                          tinyint              comment '数据来源(1-天波手环，2-米越手环，3-睡眠带子)'
) comment '每天睡眠状态数据表'
partitioned by(part_date string)


==========
create table clife_campus_ods_sports_day_data(
     data_id                         bigint               comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       date                 comment '日期'
    ,class_id                        bigint               comment '班级唯一标识'
    ,amount                          int                  comment '步数'
    ,update_time                     datetime             comment '更新时间'
) comment '运动步数日数据表'
partitioned by(part_date string)








==========
create table clife_campus_ods_fresh_air_machine_run_data(
     data_id                         bigint               comment '主键标识'
    ,device_no                       bigint               comment '设备标识'
    ,school_id                       bigint               comment '学校id'
    ,air_intake_state                tinyint              comment '设备状态（0-关闭 1-开启）'
    ,air_intake_value_state          tinyint              comment '风量大小（0-9）'
    ,air_exhaust_value_state         tinyint              comment '排风风量（0-9）'
    ,heat_state                      tinyint              comment '辅热状态（0-关闭 1-开启）'
    ,air_intake_temp                 tinyint              comment '进风口温度（0~80）°c'
    ,air_intake_humidity             tinyint              comment '进风口湿度（0~100）(%rh)'
    ,air_exhaust_temp                tinyint              comment '出风口温度（0~80）°c'
    ,air_exhaust_humidity            tinyint              comment '出风口湿度（0~100）(%rh)'
    ,co2_value                       smallint             comment 'co2值（0~5000）（ppm'
    ,pm25_value                      smallint             comment 'pm2.5值（0~1000）（μg/m3）'
    ,formaldehyde_value              smallint             comment '甲醛浓度（0~5000）（mg/m3）'
    ,air_fresh_value                 tinyint              comment '空气清新指数(0-9)'
    ,create_time                     string               comment '数据上传时间（北京时间）'
    ,upload_time                     string               comment '修改时间'
) comment '智能新风机运行数据表'
partitioned by(part_date string)



========
create table clife_campus_ods_student_attendance_daily(
     record_id                       bigint               comment '记录id'
    ,org_id                          bigint               comment '机构id'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       date                 comment '数据日期'
    ,class_id                        bigint               comment '班级唯一标识'
    ,am_card_time                    datetime             comment '入园时间(hh:mm)'
    ,pm_card_time                    datetime             comment '离园时间(hh:mm)'
    ,`result`                        tinyint              comment '状态(1-正常出席、2-缺席、3-病假、4-事假、5-未入园、6-空)'
    ,content                         string               comment '内容'
    ,update_time                     datetime             comment '更新时间'
    ,update_by_user                  bigint               comment '更新人'
    ,card_url                        string               comment '打卡图片'
    ,am_url                          string               comment '打卡图片-入园'
    ,pm_url                          string               comment '打卡图片-离园'
) comment '幼儿考勤-天数据表'
partitioned by(part_date string)



===========================
create table clife_campus_ods_student_leave(
     leave_id                        int                  comment '请假记录id'
    ,student_id                      bigint               comment '学生唯一标识'
    ,leave_type                      tinyint              comment '请假类型:2-病假,3-事假'
    ,leave_reason                    string               comment '请假事由'
    ,leave_url                       string               comment '病况资料图片url，多个url使用逗号","间隔'
    ,leave_duration                  double               comment '请假时长（单位：天）'
    ,start_time                      datetime             comment '开始时间'
    ,end_time                        datetime             comment '结束时间'
    ,start_time_type                 tinyint              comment '请假开始时间类型：0-上午 1-下午'
    ,end_time_type                   tinyint              comment '请假结束时间类型：0-上午 1-下午'
    ,teacher_data_id                 bigint               comment '审批老师主键id'
    ,read_status                     tinyint              comment '阅读状态：1-未读，2-已读'
    ,approve_status                  tinyint              comment '审批状态：1-未处理，2-已处理'
    ,approve_date                    datetime             comment '审批时间'
    ,status                          tinyint              comment '状态：0-家长撤销；1-提交成功 ; 2-老师撤销'
    ,create_date                     datetime             comment '创建时间'
    ,update_date                     datetime             comment '修改时间'
    ,org_id                          bigint               comment '学校id'
) comment '学生请假记录表'
partitioned by(part_date string)




==============幼儿心理培优记录
create table clife_campus_ods_disposition_works(
     data_id                         int                  comment '作品id自增'
    ,student_id                      int                  comment '学生id'
    ,uploader                        int                  comment '上传人用户id'
    ,teacher_id                      int                  comment '批阅的老师的id'
    ,pic_url                         string               comment '作品图片路径'
    ,status                          tinyint              comment '状态'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
    ,`comment`                       string               comment '老师评语'
    ,features                        string               comment '419返回的原始数据及匹配的特点id留根'
    ,topic                           tinyint              comment '绘画主题'
    ,source                          tinyint              comment '来源'
    ,feature_analysis                string               comment '特点解析'
    ,explanation                     string               comment '原因解释'
    ,advising                        string               comment '指导意见'
    ,reject_explanation              string               comment '驳回说明'
    ,confirmed                       tinyint              comment '是否已确认'
) comment '性格画像的学生作品表'
partitioned by(part_date string)

==========测评结果列表
create table clife_campus_ods_measurement_result(
     result_id                       int                  comment '编号'
    ,manage_info_id                  int                  comment '测评信息编号'
    ,scale_category_id               int                  comment '量表分类id'
    ,student_id                      int                  comment '学生id'
    ,start_time                      string               comment '开始时间'
    ,end_time                        string               comment '结束时间'
    ,time_length                     int                  comment '测评所花的时间(单位：s)'
    ,score                           double               comment '测评结果分'
    ,t_score                         double               comment '分'
    ,`result`                        string               comment '测评结果'
    ,`level`                         tinyint              comment '结果等级'
    ,is_normal                       tinyint              comment '是否正常'
    ,status                          tinyint              comment '状态'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
    ,recommend_name                  string               comment '推荐方案'
) comment '测评结果列表'
partitioned by(part_date string)

==============测评管理信息列表
create table clife_campus_ods_measurement_manage_info(
     manage_info_id                  int                  comment '编号'
    ,manage_info_name                string               comment '测评名称'
    ,start_time                      string               comment '量表有效开始时间'
    ,end_time                        string               comment '量表有效截止时间'
    ,sex                             tinyint              comment '性别'
    ,scale_id                        int                  comment '量表编号'
    ,result_image_url                string               comment '测评结果图访问url'
    ,image_url                       string               comment '前端缩略图访问url'
    ,status                          tinyint              comment '状态'
    ,create_time                     string               comment '创建时间'
) comment '测评管理信息列表'
partitioned by(part_date string)

===============心理分析
create table clife_campus_ods_report_disposition(
     data_id                         int                  comment '主键自增'
    ,works_id                        int                  comment '心理分析作品id'
    ,pic_url                         string               comment '作品图片'
    ,student_id                      int                  comment '学生标识'
    ,class_id                        int                  comment '班级标识'
    ,`date`                          string               comment '统计日期'
    ,tags_normal                     string               comment '正常标签'
    ,tags_negative                   string               comment '负面标签'
    ,mode_type_1                     tinyint              comment '人格为外向性下的特性id'
    ,score_1                         int                  comment '外倾性得分'
    ,mode_type_5                     tinyint              comment '人格为开放性下的特性'
    ,score_5                         int                  comment '开放性得分'
    ,mode_type_9                     tinyint              comment '人格为宜人性下的特性'
    ,score_9                         int                  comment '宜人性得分'
    ,mode_type_13                    tinyint              comment '人格为责任心下的特性'
    ,score_13                        int                  comment '责任心得分'
    ,mode_type_17                    tinyint              comment '人格为情绪稳定性下的特性'
    ,score_17                        int                  comment '情绪稳定性得分'
    ,student_name                    string               comment '学生姓名'
    ,photo_url                       string               comment '学生头像'
) comment '心理分析作品关联特性及标签统计表'
partitioned by(part_date string)

============学生心理作品的大五人格平均分
create table clife_campus_ods_disposition_score(
     data_id                         bigint               comment '数据id,自增'
    ,student_id                      bigint               comment '学生标识'
    ,mode_type_1                     int                  comment '外倾性得分'
    ,mode_type_5                     int                  comment '开放性得分'
    ,mode_type_9                     int                  comment '宜人性得分'
    ,mode_type_13                    int                  comment '责任心得分'
    ,mode_type_17                    int                  comment '情绪稳定性得分'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
    ,last_id                         int                  comment '最后统计的一个心理作品id'
) comment '学生心理作品的大五人格平均分表'
partitioned by(part_date string)

=========活动位置功能信息表
create table clife_campus_ods_interest_map_location(
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
) comment '活动位置功能信息表'
partitioned by(part_date string)

=====个人位置分析表
create table clife_campus_ods_student_location(
     data_id                         bigint               comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,map_location_id                 bigint               comment '活动位置编号'
    ,time_length                     int                  comment '学生停留时长(单位秒)'
    ,enter_time                      string               comment '进入时间'
    ,leave_time                      string               comment '离开时间'
    ,create_time                     string               comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
    ,activity_data                   tinyint              comment '活动数据表示:1(是) '
    ,source                          tinyint              comment '数据来源（1-米越，2-天波）'
) comment '个人位置分析表'
partitioned by(part_date string)



==========测温记录
create table clife_campus_ods_thermometer_data(
     thermometer_id                  bigint               comment '体温id'
    ,device_id                       string               comment '设备编号'
    ,student_id                      bigint               comment '学生唯一标识'
    ,`value`                         double               comment '体温值'
    ,`level`                         tinyint              comment '（体温正常状态）0：偏低，1：正常   2：低热，3高热'
    ,measure_time                    datetime             comment '测量时间（utc）'
    ,upload_time                     datetime             comment '上传时间(utc)'
    ,source                          tinyint              comment '数据来源：1-体温检测仪；2-家长app；3-米越耳温枪'
    ,status                          int                  comment '状态（0：无效 1：有效）'
    ,if_check                        int                  comment '状态（0：未审核 1：已审核）'
) comment '测温记录'
partitioned by(part_date string)


========身高体重
create table clife_campus_ods_student_body_data(
     body_data_id                    bigint               comment '唯一标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,device_no                       varchar              comment '设备编号'
    ,height                          double               comment '身高（cm）'
    ,weight                          double               comment '体重（kg）'
    ,`level`                         tinyint              comment '（身材正常状态）  0：偏瘦  1：标准  2：超重  3：肥胖 99-其它'
    ,height_level                    tinyint              comment '（身高状态）  0：偏矮  1：标准  2：偏高'
    ,measure_time                    datetime             comment '测量时间（utc）'
    ,upload_time                     datetime             comment '上传时间(utc)'
    ,source                          tinyint              comment '数据来源：1-身高体重仪；2-家长app；3-nb-身高体重仪；4-后台导入,5:上和身高体重仪，6：鼎恒身高体重仪'
    ,status                          int                  comment '状态（0：无效 1：有效）'
    ,if_check                        int                  comment '状态（0：未审核 1：已审核）'
    ,bmi                             double               comment 'bmi'
) comment '身高体重表'
partitioned by(part_date string)


===============
create table if not exists dwh_ods_campus.clife_campus_ods_org(
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
)comment '学校信息表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



=====================
create table if not exists dwh_ods_campus.clife_campus_ods_student(
     student_id                      bigint               comment '学生id'
    ,org_id                          bigint               comment '学校id'
    ,class_id                        bigint               comment '班级id'
    ,status                          int                  comment '学生状态（0-停用 1-正常 2-毕业 3-结业）'
    ,deleted                         int                  comment '删除 0-已删除 1-正常'
    ,student_name                    string               comment '姓名'
    ,parent_name                     string               comment '家长姓名'
    ,student_no                      string               comment '学号'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,birthday                        date                 comment '出生日期'
    ,card_no                         string               comment '身份证号码'
    ,home_address                    string               comment '家庭地址'
    ,enter_school_date               date                 comment '入学日期'
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
)comment '学生信息表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


========================
create table if not exists dwh_ods_campus.clife_campus_ods_class(
     class_id                        bigint               comment '班级id'
    ,class_no                        string               comment '班级编号'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        int                  comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,status                          int                  comment '班级状态（0-失效 1-正常 2-毕业）'
    ,enter_school_year               string               comment '入学年份'
    ,leave_date                      date                 comment '毕业离园日期'
    ,graduation_time                 datetime             comment '毕业处理时间'
    ,create_time                     datetime             comment '添加时间'
    ,update_time                     datetime             comment '修改时间'
)comment '班级信息表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


============================
create table if not exists dwh_ods_campus.clife_campus_ods_env_area(
     city_id                         bigint               comment '地级市标识'
    ,area_id                         bigint               comment '县级市、区标识'
    ,area_name                       string               comment '县级市、区名称'
    ,area_english_name               string               comment '县级市、区英文名'
    ,weather_code                    string               comment '天气编码'
)comment '地区信息表'
row format delimited fields terminated by '\t'
stored as parquet
;


=========================
create table if not exists dwh_ods_campus.clife_campus_ods_env_city(
     `province_id`                   bigint               comment '省份标识'
    ,`city_id`                       bigint               comment '城市标识'
    ,`baidu_city_code`               int                  comment '百度城市编码'
    ,`city_name`                     string               comment '城市名称'
    ,`city_english_name`             string               comment '城市英文名称'
    ,`longitude`                     double               comment '经度'
    ,`latitude`                      double               comment '纬度'
    ,`is_capital`                    int                  comment '是否为省会城市（0-否 1-是）'
)comment '城市信息表'
row format delimited fields terminated by '\t'
stored as parquet
;


============================
create table if not exists dwh_ods_campus.clife_campus_ods_env_country(
     `country_id`                    bigint               comment '国家标识'
    ,`country_name`                  string               comment '国家名称'
    ,`country_en_name`               string               comment '国家英文名'
)comment '国家信息表'
row format delimited fields terminated by '\t'
stored as parquet
;

===============================

create table if not exists dwh_ods_campus.clife_campus_ods_env_partition(
     `country_id`                    bigint               comment '国家标识'
    ,`partition_id`                  bigint               comment '地理分区标识'
    ,`partition_name`                string               comment '地理分区名称'
)comment '区域信息表'
row format delimited fields terminated by '\t'
stored as parquet
;


=================================
create table if not exists dwh_ods_campus.clife_campus_ods_env_province(
     `partition_id`                  bigint               comment '地理分区标识'
    ,`province_id`                   bigint               comment '省份标识'
    ,`province_name`                 string               comment '省份名称'
    ,`province_english_name`         string              
    ,`longitude`                     double               comment '经度'
    ,`latitude`                      double               comment '纬度'
    ,`baidu_code`                    int                  comment '百度编码'
)comment '省份信息表'
row format delimited fields terminated by '\t'
stored as parquet
;

====================
create table if not exists dwh_ods_campus.clife_campus_ods_grade(
     `data_id`                       bigint               comment '主键标识'
    ,`grade_id`                      string               comment '年级id'
    ,`org_id`                        bigint               comment '学校id'
    ,`grade_name`                    string               comment '年级名称'
    ,`grade_order`                   int                  comment '年级排序'
    ,`create_time`                   datetime             comment '添加时间'
    ,`update_time`                   datetime             comment '修改时间'
)comment '年级信息表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

======================
create table if not exists dwh_ods_campus.clife_campus_ods_class_room(
     class_room_id                   bigint               comment '教室id'
    ,class_room_name                 string               comment '教室名称（位置）'
    ,class_id                        bigint               comment '班级id'
    ,status                          tinyint              comment '班级教室状态（0-失效 1-正常）'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
    ,school_id                       bigint               comment '学校id'
)comment '教室'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;




=========================
create table if not exists dwh_ods_campus.clife_campus_ods_sports_record(
     data_id                         bigint               comment '主键标识'
    ,device_no                       string               comment '设备编号'
    ,student_id                      bigint               comment '学生唯一标识'
    ,`value`                         int                  comment '步数'
    ,`level`                         tinyint              comment '保留'
    ,source                          tinyint              comment '数据来源：1-手环；2-家长app；3-手表'
    ,data_time                       date                 comment '测量日期'
    ,measure_time                    datetime             comment '测量时间（utc）'
    ,upload_time                     datetime             comment '数据上传时间（utc）'
    ,step_type                       tinyint              comment '步数类型：81-小时步数，111-总步数'
    ,from_type                       tinyint              comment '来源类型：1-app,2-ap盒子'
)comment '运动记录'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;




=================================
create table if not exists  dwh_ods_campus.clife_campus_ods_file_allergy_relation(
     data_id                         int                  comment '唯一标识'
    ,`file_id`                       bigint               comment '学生健康档案id'
    ,allergy_id                      int                  comment '过敏源id'
    ,`status`                        tinyint              comment '0:失效，1：正常，2：停用'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '健康档案与过敏源关系表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



===================================
create table if not exists  dwh_ods_campus.clife_campus_ods_public_standard_bmi(
     bmi_id                          int                  comment '标准编号'
    ,bmi_type                        int                  comment '类型：1-bmi,2-bmi百分位,3-身高百分位，4-体重百分位,5-身高标准'
    ,bmi_sex                         int                  comment '性别(1:男，2:女 3：其他)'
    ,bmi_age                         int                  comment '年龄'
    ,`month`                         int                  comment '月份'
    ,value1                          double               comment '对应3%或者-2的线'
    ,value2                          double               comment '对应15%或者0的线'
    ,value3                          double               comment '对应50%或者1的线'
    ,value4                          double               comment '对应85%或者2的线'
    ,value5                          double               comment '对应97%的线'
)comment 'bmi的世界健康组织标准'
row format delimited fields terminated by '\t'
stored as parquet
;



=================================
create table if not exists  dwh_ods_campus.clife_campus_ods_disposition_tag(
     tag_id                          int                  comment '标签id,自增'
    ,tag_name                        string               comment '标签名'
    ,mood_type                       int                  comment '人格id'
    ,disposition_type                int                  comment '特性id'
    ,is_negative                     tinyint              comment '是否负面 1-不是 2-是 (冗余字段)'
    ,score                           int                  comment '标签分值'
    ,status                          tinyint              comment '状态 0-无效 1-草稿 2-正式'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '性格画像的标签表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


=========================
create table if not exists dwh_ods_campus.clife_campus_ods_holiday(
     data_id                         bigint               comment '主键id'
    ,org_id                          bigint               comment '学校id(id为0,表示所有学校公用节假日信息)'
    ,holiday_year                    string               comment '节假日年份'
    ,holiday_name                    string               comment '节假日名称'
    ,holiday_type                    tinyint              comment '节日类型,0-国家法定节假日 1-特殊节假日'
    ,holiday_begin_date              date                 comment '放假开始时间'
    ,holiday_end_date                date                 comment '放假结束时间'
    ,deleted                         tinyint              comment '1-正常,0-删除'
    ,create_time                     datetime             comment '创建时间'
    ,module_type                     tinyint              comment '所属模块(1-教职工、2-学生)'
    ,date_type                       tinyint              comment '日期类型(1-法定假日、2-新增假日、3-上课、4-休息，模块为学生时使用)'
)comment '节假日表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


======================
create table if not exists dwh_ods_campus.clife_campus_ods_staff_archives(
     data_id                         bigint               comment '自增主键'
    ,archives_id                     bigint               comment '档案id(同用户id)'
    ,org_id                          bigint               comment '机构id'
    ,avatar                          string               comment '头像'
    ,staff_type                      tinyint              comment '人员类型(1-教师，2-教职工)'
    ,entry_date                      date                 comment '入职日期'
    ,status                          tinyint              comment '状态（0-停用 1-正常）'
    ,deleted                         tinyint              comment '删除 0-已删除 1-正常'
    ,position_id                     bigint               comment '岗位id'
    ,dept_id                         bigint               comment '部门id'
    ,create_time                     datetime             comment '创建时间'
    ,change_type                     tinyint              comment '异动类型（1-在职,2-离职,3-辞退,4-外部调转,5-退休）'
    ,update_time                     datetime             comment '修改时间'
    ,card_no                         string               comment '卡号'
)comment '教职工表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

======================
create table clife_campus_ods_water_drink_record(
     data_id                         bigint               comment '自增主键'
    ,student_id                      bigint               comment '学生唯一id'
    ,order_id                        string               comment '刷卡时间+卡号'
    ,device_no                       string               comment '设备编号'
    ,cdate                           string               comment '刷卡时间'
    ,card_no                         string               comment '卡号'
    ,water_amount                    double               comment '出水量ml'
    ,is_auto                         tinyint              comment '1-净饮机 2-手动添加'
    ,intds                           double               comment '进水tds'
    ,outtds                          double               comment '出水tds'
    ,temperature                     double               comment '饮水温度'
    ,time_integer                    int                  comment '时分秒的整数'
    ,school_id                       bigint               comment '学校id'
    ,device_type                     tinyint              comment '净饮机类型'
    ,create_time                     string               comment '创建时间'
    ,device_position                 string               comment '饮水机位置'
)comment '饮水记录表'
partitioned by(part_date string)

======================
create table clife_campus_ods_school_device(
     data_id                         int                  comment '主键标识'
    ,device_no                       string               comment '设备编号'
    ,device_type                     tinyint              comment '设备类型'
    ,school_id                       bigint               comment '校园的机构id'
    ,position_type                   tinyint              comment '位置类型'
    ,position_id                     bigint               comment '设备位置id'
    ,bind_time                       string               comment '绑定时间'
)comment '学校公共设备'
partitioned by(part_date string)



=======================
create table  clife_campus_ods_student_allergy_ingredients(
     record_id                       int                  comment '记录id'
    ,student_id                      bigint               comment '学生唯一id'
    ,ingredient_id                   int                  comment '食材id'
    ,checkup_id                      int                  comment '幼儿健康-体检管理添加的过敏食材'
)comment '学生过敏食材表'
partitioned by(part_date string)

======================
create table clife_campus_ods_ingredients_info(
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


======================
create table clife_campus_ods_interest_activity(
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

====================
create table clife_campus_ods_water_drink_setting(
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
) comment '饮水设置表'
partitioned by(part_date string)

====================
create table clife_campus_ods_sports_assess_setting(
     data_id                         bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,grade_id                        int                  comment '年级'
    ,adequate                        int                  comment '充足'
    ,standard                        int                  comment '达标'
    ,insufficient                    int                  comment '不足'
    ,update_time                     datetime             comment '修改时间'
    ,create_time                     datetime             comment '创建时间'
) comment '活动评估设置表'
partitioned by(part_date string)


========================
create table clife_campus_ods_student_am_inspection (
     inspection_id                   bigint               comment '晨午检id'
    ,org_id                          bigint               comment '学校id'
    ,student_id                      bigint               comment '学生id'
    ,user_id                         bigint               comment '检查人id'
    ,type                            tinyint              comment '检查类型 0晨检 1午检'
    ,img                             string               comment '检查图片'
    ,is_temperature                  tinyint              comment '体温 0正常 1具体温度'
    ,temperature                     double               comment '体温值(选择具体温度才有值)'
    ,inspection_date                 date                 comment '检查日期'
    ,is_abnormal                     tinyint              comment '检查结果 0异常 1正常'
    ,other_symptom                   string               comment '其他症状'
    ,is_disease                      tinyint              comment '疾病确认(0可以确认 1无法确认)'
    ,disease_id                      bigint               comment '疾病id'
    ,infectious_disease              tinyint              comment '是否传染病(0:否，1:是，2无法确认)'
    ,disease_name                    string               comment '疾病名称'
    ,treatment                       tinyint              comment '处理情况 0家长送医 1留园观察 2停课治疗 3其他'
    ,other                           string               comment '其他情况'
    ,deleted                         tinyint              comment '是否删除 0是 1不是'
    ,status                          tinyint              comment '状态 0已确定 1待确认'
    ,data_sources                    tinyint              comment '数据来源  0 web录入  1 智能设备 2批量创建 3移动端录入'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '修改时间'
) comment '晨午检信息'
partitioned by(part_date string)


==========================================================
create table clife_campus_ods_inspection_contagion (
     inspection_id                   bigint               comment '晨午检id'
    ,contagion_id                    int                  comment '传染病id'
    ,contagion_type                  tinyint              comment '传染病类型'
    ,create_time                     datetime             comment '创建时间'
)comment '晨午检、传染病关联表'
partitioned by(part_date string)

=============================================================
create table clife_campus_ods_student_inspection_symptom (
     inspection_id                   bigint               comment '晨午检id'
    ,symptom_id                      int                  comment '症状id'
    ,symptom_type                    tinyint              comment '症状类型'
    ,create_time                     datetime             comment '创建时间'
)comment '晨午检，疾病症状关联表'
partitioned by(part_date string)



===============================================================
create table clife_campus_ods_system_dict_item (
     dict_id                         int                  comment '字典id'
    ,item_text                       string               comment '字典项文本'
    ,item_value                      string               comment '字典项值'
    ,description                     string               comment '描述'
    ,show_order                      int                  comment '排序'
    ,deleted                         tinyint              comment '0：删除，1：正常'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '字典选项';


================================================================


create table if not exists dwh_ods_campus.clife_campus_ods_into_campus_detail(
     record_id                       bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       datetime             comment '数据日期'
    ,am_status                       tinyint              comment '入园状态(0-未入园、1-已入园)'
    ,am_time                         datetime             comment '入园时间(hh:mm:ss)'
    ,`type`                          tinyint              comment '条件类型（1-考勤、2-饮水、3-步数、4-运动量、5-晨检）'
    ,create_time                     datetime             comment '创建时间'
) comment '入园明细表'
partitioned by(part_date string)


=================================================================
create table dwh_ods_campus.clife_campus_ods_sport_amount (
     data_id                         int                  comment '记录id'
    ,student_id                      bigint               comment '学生唯一标识'
    ,device_no                       varchar              comment '设备编号'
    ,measure_time                    datetime             comment '测量时间'
    ,sport_num                       int                  comment '运动量'
    ,upload_time                     datetime             comment '上传时间'
    ,source                          tinyint              comment '数据来源：1-手环；2-家长app；3-手表'
)comment '米越运动量表'
partitioned by(part_date string);

=====================================================================
create table if not exists dwh_ods_campus.clife_campus_ods_position (
     position_id                     bigint               comment '岗位id'
    ,org_id                          bigint               comment '机构id'
    ,position_name                   string               comment '岗位名称'
    ,deleted                         tinyint              comment '0：删除，1：正常'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '岗位表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;

===================================================================
create table if not exists dwh_ods_campus.clife_campus_ods_user (
     user_id                         bigint               comment '用户id'
    ,username                        string               comment '账号'
    ,real_name                       string               comment '昵称'
    ,sex                             tinyint              comment '性别'
    ,mobile                          string               comment '手机'
    ,email                           string               comment '邮箱'
    ,id_card_type                    tinyint              comment '证件类型'
    ,id_card                         string               comment '证件号码'
    ,birthday                        date                 comment '生日'
    ,nation                          tinyint              comment '民族'
    ,political_status                tinyint              comment '政治面貌'
    ,education                       tinyint              comment '最高学历'
    ,province_id                     int                  comment '省份id'
    ,city_id                         bigint               comment '城市id'
    ,area_id                         bigint               comment '区县id'
    ,province_city_area              string               comment '省市区'
    ,address                         string               comment '详细地址'
    ,clife_id                        bigint               comment '公共平台的用户id'
    ,status                          tinyint              comment '1正常'
    ,deleted                         tinyint              comment '1正常0已删除'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '更新时间'
)comment '用户基础数据表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists dwh_ods_campus.clife_campus_ods_shit_card_data (
     data_id                         bigint               comment '主键标识'
    ,device_no                       string               comment '设备编号'
    ,student_id                      bigint               comment '学生唯一标识'
    ,value                           int                  comment '数据index'
    ,card_time                       datetime             comment '刷卡时间(本地)'
    ,status                          tinyint              comment '1：有效）'
    ,upload_time                     datetime             comment '上传时间(本地)'
)comment '如厕刷卡器数据'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;




create table if not exists dwh_ods_campus.clife_campus_ods_sport_amount_original (
     data_id                         bigint               comment '记录id'
    ,student_id                      bigint               comment '学生id'
    ,device_no                       string               comment '设备编号'
    ,measure_time                    datetime             comment '测量时间'
    ,data_time                       date                 comment '数据日期'
    ,sport_num                       int                  comment '运动量'
    ,upload_time                     datetime             comment '更新时间'
    ,source                          tinyint              comment '1-手环；2-家长app；3-手表；4-米越；5-天波'
)comment '米越运动量原始记录表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table if not exists dwh_ods_campus.clife_campus_ods_student_info (
     student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,org_id                          bigint               comment '学校id'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,avatar                          string               comment '头像'
    ,birthday                        date                 comment '出生日期'
    ,sex                             tinyint              comment '3-其他'
    ,enter_school_year               char                 comment '学年'
    ,status                          tinyint              comment '3-结业）'
    ,deleted                         tinyint              comment '1-正常'
)comment '学生信息'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists dwh_ods_campus.clife_campus_ods_medicine (
     data_id                         bigint               comment '唯一标识'
    ,student_id                      bigint               comment '学生id'
    ,medication_date                 date                 comment '喂药日期'
    ,whether_inspect                 tinyint              comment '药品是否视检0:否，1：是'
    ,medication_reasons              string               comment '用药原因'
    ,number_of_feeding               tinyint              comment '喂药次数'
    ,deleted                         tinyint              comment '0：删除，1：正常'
    ,status                          tinyint              comment '1-待视检、2-已撤销、3-已视检、4-已完成'
    ,source                          tinyint              comment '0-老师录入，1-家长录入'
    ,update_time                     datetime             comment '更新时间'
    ,create_time                     datetime             comment '创建时间'
    ,parent_sign_url                 string               comment '家长签字图片'
)comment '喂药信息'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;

create table if not exists dwh_ods_campus.clife_campus_ods_medicine_details (
     data_id                         bigint               comment '唯一标识'
    ,feeding_id                      bigint               comment '喂药信息id'
    ,medicine_name                   string               comment '药物名称'
    ,medication_time                 datetime             comment '用药时间'
    ,medication_method               tinyint              comment '用药方式，0：口服，1：外用'
    ,dosage                          string               comment '用药剂量'
    ,unit                            tinyint              comment '单位，0：包，1：颗，2：滴，3：ml'
    ,picture_url                     string               comment '药物图片url'
    ,medication_status               tinyint              comment '0：待喂药，1：已喂药'
    ,status                          tinyint              comment '0：失效，1：正常，2：停用'
    ,update_time                     datetime             comment '更新时间'
    ,create_time                     datetime             comment '创建时间'
    ,moment                          tinyint              comment '0：早上，1：中午'
)comment '喂药详情'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists dwh_ods_campus.clife_campus_ods_message (
     message_id                      bigint               comment '信息编号'
    ,app_name                        string               comment '应用名称'
    ,title                           string               comment '标题'
    ,summary                         string               comment '概要，简单描述'
    ,picture_url                     string               comment '简图路径'
    ,sender                          bigint               comment '发送者(系统发送的sender为0)'
    ,icon                            string               comment '图标url'
    ,message_type                    bigint               comment 'resource_id'
    ,business_param                  string               comment '业务参数的值json格式'
    ,status                          tinyint              comment '是否有效(0:无效；1正常)'
    ,create_time                     datetime             comment '创建时间'
    ,jump_link_url                   string               comment '跳转链接(应用代码)'
    ,content_type                    tinyint              comment '跳转消息)'
    ,data_flag                       tinyint              comment '数据来源标识（默认为0-普通推送消息；1-园丁端老师；2-校园运营端）'
)comment '消息(动态提醒)'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;

create table if not exists dwh_ods_campus.clife_campus_ods_message_type (
     data_id                         int                 
    ,message_type                    bigint               comment '消息类型对应应用的resource_id'
    ,message_type_name               string               comment '消息类型名称'
    ,user_type                       tinyint              comment '2-家长'
)comment '消息类型'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;

create table if not exists dwh_ods_campus.clife_campus_ods_message_user (
     data_id                         bigint               comment '自增主键'
    ,user_id                         bigint               comment '用户id'
    ,message_id                      bigint               comment '消息id'
    ,org_id                          bigint               comment '机构id'
    ,student_id                      bigint               comment '学生id'
    ,user_type                       bigint               comment '2-家长'
    ,status                          int                  comment '消息状态（0-删除，1-未读，2-已读）'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     timestamp            comment '更新时间'
)comment '用户消息关联表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;


create table if not exists dwh_ods_campus.clife_campus_ods_user_class(
     org_id                          bigint               comment '机构id'
    ,user_id                         bigint               comment '用户id'
    ,class_id                        bigint               comment '班级id'
)comment '用户班级关联表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet;



create table `dwh_ods_campus.clife_campus_ods_city`(
     `province_id`                   int                  comment '省份id'
    ,`city_id`                       bigint               comment '城市id'
    ,`baidu_city_code`               string               comment '百度城市编码'
    ,`city_name`                     string               comment '城市名称'
    ,`city_english_name`             string               comment '城市英文名称'
    ,`longitude`                     double               comment '城市经度'
    ,`latitude`                      double               comment '城市纬度'
    ,`is_capital`                    int                  comment '是否为省会城市'
) comment '城市数据表';

create table `dwh_ods_campus.clife_campus_ods_area`(
     `city_id`                       bigint               comment '城市id'
    ,`area_id`                       string               comment '区域id'
    ,`area_name`                     string               comment '区域名称'
    ,`area_english_name`             string               comment '区域英文名称'
    ,`weather_code`                  string               comment '天气编码'
) comment '县级市、区数据表';

create table `dwh_ods_campus.clife_campus_ods_province`(
     `partition_id`                  int                  comment '县级市、区id'
    ,`province_id`                   int                  comment '省份id'
    ,`province_name`                 string               comment '省份名称'
    ,`province_english_name`         string               comment '省份英文名'
    ,`longitude`                     string               comment '省份经度'
    ,`latitude`                      string               comment '省份纬度'
    ,`baidu_code`                    int                  comment '省份百度编码'
) comment '省份数据表';

create table `dwh_ods_campus.clife_campus_ods_partition`(
     `country_id`                    int                  comment '国家id'
    ,`partition_id`                  int                  comment '县级市、区id'
    ,`partition_name`                string               comment '县级市、区名称'
) comment '中国七大地理分区';

create table `dwh_ods_campus.clife_campus_ods_country`(
     `country_id`                    int                  comment '国家id'
    ,`country_name`                  string               comment '国家名称'
    ,`country_en_name`               string               comment '国家英文名'
) comment '国家数据表';
