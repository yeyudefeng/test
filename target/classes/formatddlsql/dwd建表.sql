create table if not exists dwh_dw_campus.clife_campus_dwd_student_intocampus_info(
     record_id                       bigint               comment '入园记录'
    ,student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '学生姓名'
    ,sex                             tinyint              comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,am_status                       tinyint              comment '入园状态'
    ,att_status                      tinyint              comment '考勤入园状态'
    ,am_time                         datetime             comment '入园时间'
    ,`type`                          tinyint              comment '条件类型'
    ,create_time                     datetime             comment '创建时间'
    ,value                           int                  comment '数据值'
)comment '学生入园明细表'
partitioned by (part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

===================================


create table if not exists dwh_dw_campus.clife_campus_dwd_push_message_info(
     data_id                         bigint               comment '自增主键'
    ,user_id                         bigint               comment '用户id'
    ,user_name                       string               comment '用户名'
    ,student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '学生名称'
    ,message_id                      bigint               comment '消息id'
    ,app_name                        string               comment '应用名称'
    ,title                           string               comment '标题'
    ,summary                         string               comment '概要，简单描述'
    ,sender                          int                  comment '发送者(系统发送的sender为0)'
    ,message_type                    int                  comment '消息类型'
    ,business_param                  string               comment '业务参数的值json格式'
    ,content_type                    int                  comment '消息内容类型(0 - 富文本方式; 1 - 跳转消息)'
    ,data_flag                       int                  comment '数据来源标识（默认为0-普通推送消息；1-园丁端老师；2-校园运营端）'
    ,status                          int                  comment '消息状态（0-删除，1-未读，2-已读）'
    ,org_id                          bigint               comment '机构id'
    ,org_name                        string               comment '学校名称'
    ,grade_id                        string               comment '年级集合'
    ,grade_name                      string               comment '年级名称集合'
    ,class_id                        string               comment '班级集合'
    ,class_name                      string               comment '班级名称集合'
    ,user_type                       string               comment '用户类型'
    ,create_time                     string               comment '创建时间'
    ,update_time                     string               comment '更新时间'
)comment '消息推送明细表'
partitioned by (part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


======================================================

create table if not exists dwh_dw_campus.clife_campus_dwd_student_shit_info(
     date_id                         bigint               comment '数据id'
    ,student_id                      bigint               comment '学生唯一标识'
    ,student_name                    string               comment '学生名称'
    ,sex                             tinyint              comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,device_no                       string               comment '设备编号'
    ,shit_cnt                        int                  comment '拉臭臭次数'
    ,card_times                      string               comment '刷卡时间(本地)'
    ,length_times                    int                  comment '多次间隔时长'
    ,am_status                       tinyint              comment '入园状态'
    ,att_status                      tinyint              comment '考勤入园状态'
    ,date_time                       string               comment '上传日期'
)comment '学生拉臭臭明细表'
partitioned by (part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


======================================================

create table if not exists db_dw_campus.clife_campus_dwd_health_stu(
     student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,serious_disease_his             int                  comment '各种严重病史'
    ,serious_disease_his_d           string               comment '严重病史详情'
    ,infectious_disease_his          int                  comment '各种传染病史'
    ,infectious_disease_his_d        string               comment '传染病史详情'
    ,weight                          double               comment '体重（单位：kg）'
    ,weight_assess                   int                  comment ' 0：偏瘦  1：标准  2：超重  3：肥胖 99-其它'
    ,height                          double               comment '身高（单位：cm）'
    ,height_assess                   int                  comment ' 0：偏矮  1：标准  2：偏高'
    ,bmi                             double               comment 'bmi数值'
    ,bmi_assess                      int                  comment '0：中上，1：中等，2：中下'
    ,left_eye                        int                  comment '左眼健康'
    ,right_eye                       int                  comment '右眼健康'
    ,left_vision                     double               comment '左视力'
    ,right_vision                    double               comment '右视力'
    ,left_abnormal                   int                  comment '左视力健康'
    ,right_abnormal                  int                  comment '右视力健康'
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
    ,lack                            string               comment '接种情况'
    ,whether_allergy                 int                  comment '是否过敏体质，0：否，1：是'
    ,allergy_ingredients             string               comment '过敏食材'
)comment '学生健康明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;





=========================
create table if not exists db_dw_campus.clife_campus_dwd_robot_check_report(
     data_id                         int                  comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,student_name                    string               comment '学生姓名'
    ,sex                             int                  comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,measure_time                    string               comment '晨检时间'
    ,device_id                       bigint               comment '设备id'
    ,mac_address                     string               comment 'mac地址'
    ,product_id                      int                  comment '产品标识'
    ,card_no                         string               comment '卡号'
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
    ,access                          string               comment '是否允许入园(1-是、0-否)'
)comment '学生晨检明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


==================================
create table if not exists db_dw_campus.clife_campus_dwd_sleep_day_data(
     student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,device_no                       string               comment '设备编号'
    ,sleep_duration                  int                  comment '睡眠时长（秒）'
    ,wake_duration                   int                  comment '清醒时长（秒）'
    ,wake_number                     int                  comment '清醒次数'
    ,score                           double               comment '睡眠打分'
    ,start_time                      datetime             comment '睡眠状态开始时间'
    ,end_time                        datetime             comment '睡眠状态结束时间'
    ,upload_time                     datetime             comment '上传时间'
    ,create_time                     datetime             comment '创建时间'
    ,source                          int                  comment '数据来源(1-天波手环，3-睡眠带子)'
)comment '学生睡眠日快照表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



===========================================
create table if not exists db_dw_campus.clife_campus_dwd_sports_record(
     data_id                         bigint               comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,device_no                       string               comment '设备编号'
    ,`value`                         int                  comment '步数'
    ,`level`                         tinyint              comment '等级'
    ,source                          tinyint              comment '数据来源：1-手环；2-家长app；3-手表'
    ,measure_time                    datetime             comment '测量时间（utc）'
    ,upload_time                     datetime             comment '数据上传时间（utc）'
    ,step_type                       tinyint              comment '步数类型：81-小时步数，111-总步数'
    ,from_type                       tinyint              comment '来源类型：1-app,2-ap盒子'
)comment '学生运动明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

--饮水明细表 增量
create table if not exists dwh_dw_campus.clife_campus_dwd_water_drink_info(
     data_id                         bigint               comment '记录id'
    ,student_id                      bigint               comment '学生唯一id'
    ,student_name                    string               comment '学生姓名'
    ,sex                             tinyint              comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,device_no                       string               comment '设备编号'
    ,cdate                           datetime             comment '刷卡时间'
    ,card_no                         string               comment '卡号'
    ,water_amount                    double               comment '出水量ml-10-2'
    ,is_auto                         tinyint              comment '是否自动'
    ,intds                           double               comment '进水tds-6-2'
    ,outtds                          double               comment '出水tds-6-2'
    ,temperature                     double               comment '饮水温度-6-2'
    ,time_integer                    int                  comment '时分秒整数'
    ,device_type                     tinyint              comment '净饮机类型'
    ,create_time                     datetime             comment '创建时间'
    ,device_position                 string               comment '饮水机位置'
) comment '学生饮水明细事实表'
partitioned by (
    ,`part_date`                     string               comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;

====================

--测温明细表 增量
create table if not exists dwh_dw_campus.clife_campus_dwd_thermometer_info(
     thermometer_id                  bigint               comment '记录id'
    ,student_id                      bigint               comment '学生唯一id'
    ,student_name                    string               comment '学生姓名'
    ,sex                             tinyint              comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,device_id                       string               comment '设备编号'
    ,value                           float                comment '体温值'
    ,level                           tinyint              comment '体温正常状态0偏低1正常2低热3高热'
    ,measure_time                    datetime             comment '测量时间'
    ,upload_time                     datetime             comment '上传时间'
    ,source                          tinyint              comment '数据来源'
    ,status                          int                  comment '是否有效0无效1有效'
    ,if_check                        int                  comment '0未审核1已审核'
) comment '学生测温明细事实表'
partitioned by (
    ,`part_date`                     string               comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;

====================

--考勤明细表
create table if not exists dwh_dw_campus.clife_campus_dwd_student_attendance_info(
     student_id                      bigint               comment '学生唯一id'
    ,student_name                    string               comment '学生姓名'
    ,sex                             tinyint              comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,data_time                       datetime             comment '数据日期'
    ,am_card_time                    datetime             comment '入园时间'
    ,pm_card_time                    datetime             comment '离园时间'
    ,result                          tinyint              comment '状态(1-正常出席、2-缺席、3-病假、4-事假、5-未入园、6-空)'
    ,content                         string               comment '内容'
    ,update_time                     datetime             comment '更新时间'
    ,update_by_user                  bigint               comment '更新人'
    ,card_url                        string               comment '打卡图片'
    ,am_url                          string               comment '打卡图片-入园'
    ,pm_url                          string               comment '打卡图片-离园'
) comment '学生入园出勤明细事实表'
partitioned by (
    ,`part_date`                     string               comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;



==============================
create table if not exists dwh_dw_campus.clife_campus_dwd_classroom_env_info(
     class_room_id                   bigint               comment '教室id'
    ,class_room_name                 string               comment '教室名称'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
    ,intds                           double               comment '进水tds'
    ,intds_result                    string               comment '进水tds结果'
    ,outtds                          double               comment '出水tds'
    ,outtds_result                   string               comment '出水tds结果'
    ,air_exhaust_temp                tinyint              comment '温度'
    ,air_exhaust_result              string               comment '温度结论'
    ,air_exhaust_humidity            tinyint              comment '湿度'
    ,air_exhausthum_result           string               comment '湿度结论'
    ,co2_value                       tinyint              comment 'co2'
    ,co2_result                      string               comment 'co2结论'
    ,pm25_value                      tinyint              comment 'pm2.5'
    ,pm25_result                     string               comment 'pm2.5结论'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
)comment '班级环境卫生管理明细多事务事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



create table if not exists dwh_dw_campus.clife_campus_dwd_location_stu(
     data_id                         int                  comment '标识'
    ,map_location_id                 bigint               comment '活动位置编号'
    ,location_name                   string               comment '活动位置名称'
    ,`type`                          int                  comment '类型'
    ,time_length                     int                  comment '学生停留时长(单位秒)'
    ,enter_time                      string               comment '进入时间'
    ,leave_time                      string               comment '离开时间'
    ,activity_data                   int                  comment '活动数据表示:1(是) '
    ,source                          int                  comment '数据来源（1-米越，2-天波）'
    ,student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '性别'
    ,age                             int                  comment '年龄'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
)comment '学生区角活动明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



create table if not exists dwh_dw_campus.clife_campus_dwd_disposition_stu_info(
     student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
    ,mens_score_1                    double               comment '外向性测评结果分'
    ,mens_t_score_1                  double               comment '外向性测评结果t分'
    ,mens_level_1                    string               comment '外向性测评结果'
    ,mens_result_1                   tinyint              comment '外向性结果等级'
    ,mens_is_normal_1                tinyint              comment '外向性是否正常'
    ,mens_recommend_name_1           string               comment '外向性推荐方案'
    ,manage_info_name_1              string               comment '外向性测评名称'
    ,start_time_1                    string               comment '外向性有效开始测评时间'
    ,end_time_1                      string               comment '外向性有效截止时间'
    ,mens_score_5                    double               comment '开放性测评结果分'
    ,mens_t_score_5                  double               comment '开放性测评结果t分'
    ,mens_result_5                   string               comment '开放性测评结果'
    ,mens_level_5                    tinyint              comment '开放性结果等级'
    ,mens_is_normal_5                tinyint              comment '开放性是否正常'
    ,mens_recommend_name_5           string               comment '开放性推荐方案'
    ,manage_info_name_5              string               comment '开放性测评名称'
    ,start_time_5                    string               comment '开放性有效开始测评时间'
    ,end_time_5                      string               comment '开放性有效截止时间'
    ,mens_score_9                    double               comment '宜人性测评结果分'
    ,mens_t_score_9                  double               comment '宜人性测评结果t分'
    ,mens_result_9                   string               comment '宜人性测评结果'
    ,mens_level_9                    tinyint              comment '宜人性结果等级'
    ,mens_is_normal_9                tinyint              comment '宜人性是否正常'
    ,mens_recommend_name_9           string               comment '宜人性推荐方案'
    ,manage_info_name_9              string               comment '宜人性测评名称'
    ,start_time_9                    string               comment '宜人性有效开始测评时间'
    ,end_time_9                      string               comment '宜人性有效截止时间'
    ,mens_score_13                   double               comment '责任心测评结果分'
    ,mens_t_score_13                 double               comment '责任心测评结果t分'
    ,mens_result_13                  string               comment '责任心测评结果'
    ,mens_level_13                   tinyint              comment '责任心结果等级'
    ,mens_is_normal_13               tinyint              comment '责任心是否正常'
    ,mens_recommend_name_13          string               comment '责任心推荐方案'
    ,manage_info_name_13             string               comment '责任心测评名称'
    ,start_time_13                   string               comment '责任心有效开始测评时间'
    ,end_time_13                     string               comment '责任心有效截止时间'
    ,mens_score_17                   double               comment '情绪稳定性测评结果分'
    ,mens_t_score_17                 double               comment '情绪稳定性测评结果t分'
    ,mens_result_17                  string               comment '情绪稳定性测评结果'
    ,mens_level_17                   tinyint              comment '情绪稳定性结果等级'
    ,mens_is_normal_17               tinyint              comment '情绪稳定性是否正常'
    ,mens_recommend_name_17          string               comment '情绪稳定性推荐方案'
    ,manage_info_name_17             string               comment '情绪稳定性测评名称'
    ,start_time_17                   string               comment '情绪稳定性有效开始测评时间'
    ,end_time_17                     string               comment '情绪稳定性有效截止时间'
    ,mode_type_1                     tinyint              comment '人格为外向性下的特性id'
    ,mode_type_score_1               int                  comment '外倾性得分'
    ,mode_type_5                     tinyint              comment '人格为开放性下的特性id'
    ,mode_type_score_5               int                  comment '开放性得分'
    ,mode_type_9                     tinyint              comment '人格为宜人性下的特性id'
    ,mode_type_score_9               int                  comment '宜人性得分'
    ,mode_type_13                    tinyint              comment '人格为责任心下的特性id'
    ,mode_type_score_13              int                  comment '责任心得分'
    ,mode_type_17                    tinyint              comment '人格为情绪稳定性下的特性id'
    ,mode_type_score_17              int                  comment '情绪稳定性得分'
    ,tags_normal                     string               comment '正常标签'
    ,tags_negative                   string               comment '负面标签'
    ,synthesis_score_1               int                  comment '综合外倾性人格得分'
    ,synthesis_score_5               int                  comment '综合开放性人格得分'
    ,synthesis_score_9               int                  comment '综合宜人性人格得分'
    ,synthesis_score_13              int                  comment '综合责任心人格得分'
    ,synthesis_score_17              int                  comment '综合情绪稳定性人格得分'
)comment '学生心理多事务事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

=============================================
create table if not exists dwh_dw_campus.clife_campus_dwd_inspection_stu(
     inspection_id                   bigint               comment '晨午检id'
    ,student_id                      bigint               comment '学生id'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '性别'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,user_id                         bigint               comment '检查人id'
    ,type                            tinyint              comment '检查类型 0晨检 1午检'
    ,img                             string               comment '检查图片'
    ,is_temperature                  tinyint              comment '体温 0正常 1具体温度'
    ,temperature                     double               comment '体温值(选择具体温度才有值)'
    ,inspection_date                 string               comment '检查日期'
    ,is_abnormal                     tinyint              comment '检查结果 0异常 1正常'
    ,fever                           tinyint              comment '发热'
    ,throat                          tinyint              comment '咽部红肿'
    ,hfmd                            tinyint              comment '疑似手口足病'
    ,abnormal_rash                   tinyint              comment '异常皮疹'
    ,cough                           tinyint              comment '咳嗽'
    ,runny_nose                      tinyint              comment '流涕'
    ,rash                            tinyint              comment '皮疹'
    ,vomit                           tinyint              comment '呕吐'
    ,conjunctival                    tinyint              comment '结膜充血，分泌物多'
    ,nose_bleeding                   tinyint              comment '流鼻血'
    ,trauma                          tinyint              comment '外伤'
    ,diarrhea                        tinyint              comment '腹泻'
    ,cheek                           tinyint              comment '腮腺肿大'
    ,hand                            tinyint              comment '手部异常'
    ,mouth                           tinyint              comment '口腔异常'
    ,red_eye                         tinyint              comment '红眼'
    ,spirit                          tinyint              comment '精神不佳'
    ,fingernail                      tinyint              comment '指甲过长'
    ,contagions                      string               comment '传染病ids'
    ,influenza                       tinyint              comment '流行性感冒'
    ,rubella                         tinyint              comment '风疹'
    ,contagion_diarrhea              tinyint              comment '感染性腹泻'
    ,contagion_hfmd                  tinyint              comment '手口足病'
    ,dysentery                       tinyint              comment '痢疾'
    ,other_contagions                tinyint              comment '其他传染病'
    ,other_symptom                   string               comment '其他症状'
    ,treatment                       tinyint              comment '处理情况 0家长送医 1留园观察 2停课治疗 3其他'
    ,other                           string               comment '其他情况'
    ,deleted                         tinyint              comment '是否删除 0是 1不是'
    ,status                          tinyint              comment '状态 0已确定 1待确认'
    ,data_sources                    tinyint              comment '数据来源  0 web录入  1 智能设备 2批量创建 3移动端录入'
    ,create_time                     datetime             comment '创建时间'
    ,update_time                     datetime             comment '修改时间'
)comment '学生晨午检明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


create table if not exists dwh_dw_campus.clife_campus_dim_into_campus_detail(
     record_id                       bigint               comment '唯一标识id'
    ,org_id                          bigint               comment '机构'
    ,student_id                      bigint               comment '学生唯一标识'
    ,data_time                       datetime             comment '数据日期'
    ,am_status                       tinyint              comment '入园状态(0-未入园、1-已入园)'
    ,am_time                         datetime             comment '入园时间(hh:mm:ss)'
    ,`type`                          tinyint              comment '条件类型（1-考勤、2-饮水、3-步数、4-运动量、5-晨检）'
    ,create_time                     datetime             comment '创建时间'
)comment '运动配置维表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



====================
create table if not exists db_dw_campus.clife_campus_dwd_sports_amount(
     data_id                         bigint               comment '主键标识'
    ,student_id                      bigint               comment '学生唯一标识'
    ,student_name                    string               comment '姓名'
    ,sex                             int                  comment '1-男 2-女 3-其他'
    ,age                             int                  comment '年龄'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,device_no                       string               comment '设备编号'
    ,sport_num                       int                  comment '运动量'
    ,source                          tinyint              comment '数据来源：1-手环；2-家长app；3-手表'
    ,measure_time                    datetime             comment '测量时间'
    ,upload_time                     datetime             comment '数据上传时间'
)comment '学生运动量明细事实表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

====================
create table if not exists dwh_dw_campus.clife_campus_dwd_medicine_data    (
     `data_id`                       bigint               comment "唯一标识"                                   
    ,`medication_date`               string               comment "喂药日期"                                   
    ,`whether_inspect`               tinyint              comment "药品是否视检,0:否，1：是"                   
    ,`medication_reasons`            string               comment "用药原因"                                   
    ,`number_of_feeding`             tinyint              comment "喂药次数"                                   
    ,`deleted`                       tinyint              comment "0：删除，1：正常"                           
    ,`status`                        tinyint              comment "1-待视检、2-已撤销、3-已视检、4-已完成"     
    ,`source`                        tinyint              comment "0-老师录入，1-家长录入"                     
    ,`update_time`                   string               comment "更新时间"                                   
    ,`create_time`                   string               comment "创建时间"                                   
    ,`parent_sign_url`               string               comment "家长签字图片"                               
    ,`medicine_name_list`            string               comment "药物名称"                                   
    ,`medication_time_list`          string               comment "用药时间"                                   
    ,`medication_method_list`        string               comment "用药方式，0：口服，1：外用"                 
    ,`dosage_list`                   string               comment "用药剂量"                                   
    ,`unit_list`                     string               comment "单位，0：包，1：颗，2：滴，3：ml"           
    ,`picture_url_list`              string               comment "药物图片url"                                
    ,`uncomplete_medicine_cnt`       tinyint              comment "未喂药次数"                                 
    ,`complete_medicine_cnt`         tinyint              comment "已喂药次数"                                 
    ,`total_cnt`                     tinyint              comment "总共喂药次数"                               
    ,`am_cnt`                        tinyint              comment "早上喂药次数"                               
    ,`mid_cnt`                       tinyint              comment "中午喂药次数"                               
    ,`student_id`                    bigint               comment '学生唯一id'                                 
    ,`student_name`                  string               comment '学生姓名'                                   
    ,`sex`                           tinyint              comment '性别'                                       
    ,`age`                           int                  comment '年龄'                                       
    ,`class_id`                      bigint               comment '班级id'                                     
    ,`class_name`                    string               comment '班级名称'                                   
    ,`grade_id`                      tinyint              comment '年级id'                                     
    ,`grade_name`                    string               comment '年级名称'                                   
    ,`org_id`                        bigint               comment '学校id'                                     
    ,`org_name`                      string               comment '学校名称'                                   
    ,`area_id`                       bigint               comment '学校区县id'                                 
    ,`am_status`                     tinyint              comment '是否入园'                                   
    ,`medication_part_date`          string               comment '喂药分区日期，（等同于喂药日期）'
) comment '喂药学生表'
partitioned by (part_date string comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;
