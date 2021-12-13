create table if not exists dwh_dw_campus.clife_campus_dws_health_stu(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,serious_disease_his_cnt         int                  comment '有严重病史人数'
    ,infectious_disease_his_cnt      int                  comment '有传染病史人数'
    ,weight_fat_cnt                  int                  comment '体重肥胖人数'
    ,weight_high_cnt                 int                  comment '体重超重人数'
    ,weight_mid_cnt                  int                  comment '体重标准人数'
    ,weight_low_cnt                  int                  comment '体重偏瘦人数'
    ,height_high_cnt                 int                  comment '身高偏高人数'
    ,height_mid_cnt                  int                  comment '身高标准人数'
    ,height_low_cnt                  int                  comment '身高偏矮人数'
    ,bmi_fat_cnt                     int                  comment 'bmi肥胖人数'
    ,bmi_high_cnt                    int                  comment 'bmi超重人数'
    ,bmi_mid_cnt                     int                  comment 'bmi标准人数'
    ,bmi_low_cnt                     int                  comment 'bmi偏瘦人数'
    ,left_eye_nd_cnt                 int                  comment '左眼健康未检测人数'
    ,left_eye_normal_cnt             int                  comment '左眼健康正常人数'
    ,left_eye_blister_cnt            int                  comment '左眼砂眼人数'
    ,left_eye_conjunctivitis_cnt     int                  comment '左眼结膜炎人数'
    ,right_eye_nd_cnt                int                  comment '右眼健康未检测人数'
    ,right_eye_normal_cnt            int                  comment '右眼健康正常人数'
    ,right_eye_blister_cnt           int                  comment '右眼砂眼人数'
    ,right_eye_conjunctivitis_cnt    int                  comment '右眼结膜炎人数'
    ,left_vision_normal_cnt          int                  comment '左眼视力健康人数'
    ,left_vision_abnormal_cnt        int                  comment '左眼视力异常人数'
    ,right_vision_normal_cnt         int                  comment '右眼视力健康人数'
    ,right_vision_abnormal_cnt       int                  comment '右眼视力异常人数'
    ,caries_cnt                      int                  comment '患龋齿人数'
    ,head_neck_nd_cnt                int                  comment ' 头颈未检测人数'
    ,head_neck_normal_cnt            int                  comment ' 头颈健康正常人数'
    ,head_neck_abnormal_cnt          int                  comment ' 头颈健康异常人数'
    ,thoracic_nd_cnt                 int                  comment '胸廓未检测人数'
    ,thoracic_normal_cnt             int                  comment '胸廓健康正常人数'
    ,thoracic_abnormal_cnt           int                  comment '胸廓健康异常人数'
    ,spine_nd_cnt                    int                  comment '脊椎未检测人数'
    ,spine_normal_cnt                int                  comment '脊椎健康正常人数'
    ,spine_abnormal_cnt              int                  comment '脊椎健康异常人数'
    ,limbs_nd_cnt                    int                  comment '四肢未检测人数'
    ,limbs_normal_cnt                int                  comment '四肢健康正常人数'
    ,limbs_abnormal_cnt              int                  comment '四肢健康异常人数'
    ,pharynx_nd_cnt                  int                  comment '咽部未检测人数 '
    ,pharynx_normal_cnt              int                  comment '咽部健康正常人数'
    ,pharynx_abnormal_cnt            int                  comment '咽部健康异常人数'
    ,heart_nd_cnt                    int                  comment '心脏未检测人数'
    ,heart_normal_cnt                int                  comment '心脏健康正常人数'
    ,heart_abnormal_cnt              int                  comment '心脏健康异常人数'
    ,lung_nd_cnt                     int                  comment '肺健康未检测人数'
    ,lung_normal_cnt                 int                  comment '肺健康正常人数'
    ,lung_abnormal_cnt               int                  comment '肺健康异常人数'
    ,liver_nd_cnt                    int                  comment '肝健康未检测人数'
    ,liver_normal_cnt                int                  comment '肝健康正常人数'
    ,liver_abnormal_cnt              int                  comment '肝健康异常人数'
    ,spleen_nd_cnt                   int                  comment '脾健康未检测人数'
    ,spleen_normal_cnt               int                  comment '脾健康正常人数'
    ,spleen_abnormal_cnt             int                  comment '脾健康异常人数'
    ,alt_nd_cnt                      int                  comment '丙氨酸氨基转移酶未检测人数'
    ,alt_normal_cnt                  int                  comment '丙氨酸氨基转移酶正常人数'
    ,alt_abnormal_cnt                int                  comment '丙氨酸氨基转移酶异常人数'
    ,lack_cnt                        int                  comment '接种疫苗人数'
    ,whether_allergy_cnt             int                  comment '过敏体质人数'
    ,no_whether_allergy_cnt          int                  comment '非过敏体质人数'
    ,allergy_ingredients_cnt         int                  comment '过敏食材人数'
    ,student_cnt                     int                  comment '学生人数'
    ,left_right_normal               int                  comment '左右视力正常'
    ,left_right_abnormal             int                  comment '左右视力异常'
    ,weight_fat_assess_man           int                  comment '男3：肥胖 '
    ,weight_fat_assess_woman         int                  comment '女3：肥胖 '
    ,weight_high_assess_man          int                  comment '男2：超重 '
    ,weight_high_assess_woman        int                  comment '女2：超重 '
    ,weight_mid_assess_man           int                  comment '男1：标准 '
    ,weight_mid_assess_woman         int                  comment '女1：标准 '
    ,weight_low_assess_man           int                  comment '男0：偏瘦 '
    ,weight_low_assess_woman         int                  comment '女0：偏瘦 '
    ,hight_high_assess_man           int                  comment '男偏高'
    ,hight_high_assess_woman         int                  comment '女偏高'
    ,hight_mid_assess_man            int                  comment '男标准'
    ,hight_mid_assess_woman          int                  comment '女标准'
    ,hight_low_assess_man            int                  comment '男偏矮'
    ,hight_low_assess_woman          int                  comment '女偏矮'
)comment '学生健康汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;





====================
create table if not exists dwh_dw_campus.clife_campus_dws_robot_check_report(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,check_cnt                       int                  comment '晨检人数'
    ,no_check_cnt                    int                  comment '未晨检人数'
    ,many_check_cnt                  int                  comment '多次晨检人数'
    ,result_normal_cnt               int                  comment '晨检结果正常人数'
    ,result_abnormal_cnt             int                  comment '晨检结果异常人数'
    ,temperature_low_cnt             int                  comment '体温偏低人数'
    ,temperature_normal_cnt          int                  comment '体温正常人数'
    ,temperature_low_fever_cnt       int                  comment '体温低热人数'
    ,temperature_high_cnt            int                  comment '体温高热人数'
    ,hand_normal_cnt                 int                  comment '手部正常人数'
    ,hand_abnormal_cnt               int                  comment '手部异常人数'
    ,mouth_normal_cnt                int                  comment '口腔正常人数'
    ,mouth_abnormal_cnt              int                  comment '口腔异常人数'
    ,no_red_eye_cnt                  int                  comment '未患红眼人数'
    ,red_eye_cnt                     int                  comment '患红眼人数'
    ,no_fever_cnt                    int                  comment '未发烧人数'
    ,fever_cnt                       int                  comment '发烧人数'
    ,cheek_normal_cnt                int                  comment '未腮部肿大人数'
    ,cheek_abnormal_cnt              int                  comment '腮部肿大人数'
    ,spirit_normal_cnt               int                  comment '精神面貌正常人数'
    ,spirit_abnormal_cnt             int                  comment '精神不佳人数'
    ,throat_normal_cnt               int                  comment '咽部正常人数'
    ,throat_abnormal_cnt             int                  comment '咽炎人数'
    ,no_trauma_cnt                   int                  comment '未患外伤人数'
    ,trauma_cnt                      int                  comment '外伤人数'
    ,no_cough_cnt                    int                  comment '未患咳嗽人数'
    ,cough_cnt                       int                  comment '咳嗽人数'
    ,fingernail_normal_cnt           int                  comment '指甲长度正常人数'
    ,fingernail_abnormal_cnt         int                  comment '指甲过长人数'
    ,no_tooth_decay_cnt              int                  comment '未患蛀牙人数'
    ,tooth_decay_cnt                 int                  comment '患蛀牙人数'
    ,no_other_cnt                    int                  comment '未有其他异常人数'
    ,other_cnt                       int                  comment '有其他异常人数'
    ,access_cnt                      int                  comment '允许入园人数'
    ,student_cnt                     int                  comment '学生人数'
)comment '班级晨检汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



=================================
create table if not exists dwh_dw_campus.clife_campus_dws_sleep_day_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,sum_sleep_duration              int                  comment '睡眠总时长（秒）'
    ,sum_wake_duration               int                  comment '清醒总时长（秒）'
    ,sum_wake_number                 int                  comment '清醒总次数'
    ,score_0                         int                  comment '0星人数'
    ,score_1                         int                  comment '1星人数'
    ,score_2                         int                  comment '2星人数'
    ,score_3                         int                  comment '3星人数'
    ,score_4                         int                  comment '4星人数'
    ,score_5                         int                  comment '5星人数'
)comment '班级睡眠汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


=========================
create table if not exists dwh_dw_campus.clife_campus_dws_sports_data(
     student_id                      bigint               comment '学生唯一标识'
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
    ,sum_step                        int                  comment '总步数'
    ,is_enter                        int                  comment '是否入园（0否1是）'
    ,access_status                   int                  comment '达标状态（0不足1达标2充足）'
    ,sport_num                       int                  comment '总运动量'
)comment '学生运动汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

==========================
create table if not exists dwh_dw_campus.clife_campus_dws_classroom_env_converge(
     grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
    ,intds_high_num                  int                  comment '原水水质优数量'
    ,intds_middle_num                int                  comment '原水水质良数量'
    ,intds_qualified_num             int                  comment '原水水质合格数量'
    ,intds_general_num               int                  comment '原水水质一般数量'
    ,outtds_high_num                 int                  comment '出水水质极佳数量'
    ,outtds_middle_num               int                  comment '出水水质优秀数量'
    ,outtds_qualified_num            int                  comment '出水水质良好数量'
    ,outtds_general_num              int                  comment '出水水质一般数量'
    ,outtds_need_improve_num         int                  comment '出水水质有待改善数量'
    ,pm25_0_35_num                   int                  comment 'pm2.5优数量'
    ,pm25_36_75_num                  int                  comment 'pm2.5良数量'
    ,pm25_76_115_num                 int                  comment 'pm2.5轻度数量'
    ,pm25_116_150_num                int                  comment 'pm2.5中度数量'
    ,pm25_151_250_num                int                  comment 'pm2.5重度数量'
    ,air_exhaust_5_num               int                  comment '温度寒冷数量'
    ,air_exhaust_6_18_num            int                  comment '湿度较冷数量'
    ,air_exhaust_19_28_num           int                  comment '湿度适宜数量'
    ,air_exhaust_29_32_num           int                  comment '湿度较热数量'
    ,air_exhaust_33_num              int                  comment '湿度炎热数量'
    ,air_exhausthum_0_40_num         int                  comment '湿度干燥数量'
    ,air_exhausthum_41_75_num        int                  comment '湿度适宜数量'
    ,air_exhausthum_76_100_num       int                  comment '湿度湿润数量'
    ,co2_0_450_num                   int                  comment 'co2优数量'
    ,co2_451_800_num                 int                  comment 'co2良数量'
    ,co2_801_1200_num                int                  comment 'co2轻度数量'
    ,co2_1201_2000_num               int                  comment 'co2中度数量'
    ,co2_2001_num                    int                  comment 'co2重度数量'
)comment '年级环境卫生汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

==============================


create table if not exists dwh_dw_campus.clife_campus_dws_disposition_stu_converge(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
    ,sub_count                       int                  comment '学生人数'
    ,measurement_num                 int                  comment '量表测量人数'
    ,disposition_num                 int                  comment '绘画分析测量人数'
    ,synthesis_1_introversion_num    int                  comment '综合外倾性内向人数'
    ,synthesis_1_neutral_num         int                  comment '综合外倾性中性人数'
    ,synthesis_1_extroversion_num    int                  comment '综合外倾性外向人数'
    ,synthesis_1_introversion_rate   double               comment '综合外倾性内向占比'
    ,synthesis_1_neutral_rate        double               comment '综合外倾性中性占比'
    ,synthesis_1_extroversion_rate   double               comment '综合外倾性外向占比'
    ,synthesis_5_pragmatic_num       int                  comment '综合开放性务实人数'
    ,synthesis_5_neutral_num         int                  comment '综合开放性中性人数'
    ,synthesis_5_create_num          int                  comment '综合开放性创造人数'
    ,synthesis_5_pragmatic_rate      double               comment '综合开放性务实占比'
    ,synthesis_5_neutral_rate        double               comment '综合开放性中性占比'
    ,synthesis_5_create_rate         double               comment '综合开放性创造占比'
    ,synthesis_9_strict_num          int                  comment '综合宜人性严格人数'
    ,synthesis_9_neutral_num         int                  comment '综合宜人性中性人数'
    ,synthesis_9_broad_num           int                  comment '综合宜人性宽容人数'
    ,synthesis_9_strict_rate         double               comment '综合宜人性严格占比'
    ,synthesis_9_neutral_rate        double               comment '综合宜人性中性占比'
    ,synthesis_9_broad_rate          double               comment '综合宜人性宽容占比'
    ,synthesis_13_casual_num         int                  comment '综合责任心随性人数'
    ,synthesis_13_neutral_num        int                  comment '综合责任心中性人数'
    ,synthesis_13_order_num          int                  comment '综合责任心有序人数'
    ,synthesis_13_casual_rate        double               comment '综合责任心随性占比'
    ,synthesis_13_neutral_rate       double               comment '综合责任心中性占比'
    ,synthesis_13_order_rate         double               comment '综合责任心有序占比'
    ,synthesis_17_stable_num         int                  comment '综合情绪稳定性稳定人数'
    ,synthesis_17_neutral_num        int                  comment '综合情绪稳定性中性人数'
    ,synthesis_17_rich_num           int                  comment '综合情绪稳定性丰富人数'
    ,synthesis_17_stable_rate        double               comment '综合情绪稳定性稳定占比'
    ,synthesis_17_neutral_rate       double               comment '综合情绪稳定性中性占比'
    ,synthesis_17_rich_rate          double               comment '综合情绪稳定性丰富占比'
)comment '班级心理汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

===================================


create table if not exists dwh_dw_campus.clife_campus_dws_location_stu_converge(
     student_id                      bigint               comment '学生id'
    ,map_location_id                 bigint               comment '活动位置编号'
    ,location_name                   string               comment '活动位置名称'
    ,`type`                          int                  comment '类型'
    ,activity_day                    string               comment '活动日期'
    ,student_name                    string               comment '姓名'
    ,class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,org_area_id                     bigint               comment '学校县区id'
    ,org_area_name                   string               comment '学校县区名称'
    ,stop_time                       int                  comment '停留总时长(单位秒)'
    ,stop_count                      int                  comment '停留总次数'
)comment '学生区角活动汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;

--==========饮水汇总===============
create table if not exists dwh_dw_campus.clife_campus_dws_drink_water_data(
     student_id                      bigint               comment '学生唯一标识'
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
    ,sum_drink                       double               comment '饮水总量-10-2'
    ,no_drink                        int                  comment '饮水次数'
    ,am_status                       tinyint              comment '是否入园 0-未入园 1-已入园'
    ,standard_status                 tinyint              comment '达标情况 0-不足 1-达标 1-充足'
)comment '饮水汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


--==========测温汇总===============
create table if not exists dwh_dw_campus.clife_campus_dws_thermometer_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,student_cnt                     int                  comment '班级总学生人数'
    ,measure_student_cnt             int                  comment '测温人数'
    ,measure_cnt                     int                  comment '测温次数'
    ,no_measure_student_cnt          int                  comment '未测温人数'
    ,low_fever_num                   int                  comment '温度偏低人数'
    ,normal_num                      int                  comment '温度正常人数'
    ,low_normal_num                  int                  comment '温度低热人数'
    ,high_fever_num                  int                  comment '温度高烧人数'
)comment '测温汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;


--==========考勤汇总===============
create table if not exists dwh_dw_campus.clife_campus_dws_attendance_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '区县id'
    ,no_student                      int                  comment '学生人数'
    ,no_attendance                   int                  comment '出勤人数'
    ,no_absenteeism                  int                  comment '缺勤人数'
    ,attendance                      double               comment '出勤率'
    ,absenteeism                     double               comment '缺勤率'
    ,no_affair_leave                 int                  comment '请事假人数'
    ,no_sick_leave                   int                  comment '请病假人数'
    ,no_leave                        int                  comment '请假人数'
    ,percent_affair_leave            double               comment '请事假人数占比'
    ,percent_sick_leave              double               comment '请病假人数占比'
    ,data_date                       string               comment '数据日期'
)comment '考勤汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;



=====================================================
create table if not exists dwh_dw_campus.clife_campus_dws_inspection_stu(
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
    ,am_check_cnt                    int                  comment '晨检次数'
    ,pm_check_cnt                    int                  comment '午检次数'
    ,inspection_date                 string               comment '检查日期'
    ,is_abnormal                     tinyint              comment '检查结果异常次数'
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
)comment '学生晨午检汇总表'
partitioned by(part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;




=====================================================
create table if not exists dwh_dw_campus.clife_campus_dws_medicine_data(
     `class_id`                      bigint               comment '班级id'                                     
    ,`class_name`                    string               comment '班级名称'                                   
    ,`grade_id`                      tinyint              comment '年级id'                                     
    ,`grade_name`                    string               comment '年级名称'                                   
    ,`org_id`                        bigint               comment '学校id'                                     
    ,`org_name`                      string               comment '学校名称'                                   
    ,`area_id`                       bigint               comment '学校区县id'                                 
    ,`uncomplete_medicine_cnt`       tinyint              comment '班级未喂药次数（人次）'                                 
    ,`complete_medicine_cnt`         tinyint              comment '班级已喂药次数（人次）'                                 
    ,`total_cnt`                     tinyint              comment '班级总共喂药次数（人次）'                               
    ,`am_cnt`                        tinyint              comment '班级早上喂药次数（人次）'                               
    ,`mid_cnt`                       tinyint              comment '班级中午喂药次数（人次）'                               
    ,`medicine_person_cnt`           tinyint              comment '班级喂药总人数（包含未喂药和已喂药状态）'               
    ,`medicine_person_in_cnt`        tinyint              comment '班级喂药总人数（包含未喂药和已喂药状态，未入园）'       
    ,`medicine_person_out_cnt`       tinyint              comment '班级喂药总人数（包含未喂药和已喂药状态，已入园）'       
    ,`medication_part_date`          string               comment '喂药分区日期，（等同于喂药日期）'
) comment '喂药班级汇总表'
partitioned by (part_date string comment '喂药时间')
row format delimited fields terminated by '\t'
stored as parquet
;


=====================================================
create table if not exists dwh_dw_campus.clife_campus_dws_push_message_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,message_cnt                     int                  comment '消息总数'
    ,user_num                        int                  comment '推送用户次数'
    ,user_cnt                        int                  comment '推送用户人数'
    ,parents_num                     int                  comment '推送家长次数'
    ,parents_cnt                     int                  comment '推送家长人数'
    ,student_cnt                     int                  comment '学生人数'
    ,read_num                        int                  comment '推送消息已读数'
    ,no_read_num                     int                  comment '推送消息未读数'
    ,delete_num                      int                  comment '推送消息删除数'
    ,ordinary_num                    int                  comment '普通推送消息数'
    ,gardener_num                    int                  comment '园丁端老师推送数'
    ,campus_num                      int                  comment '校园运营端数'
    ,user_child_leave_num            int                  comment '用户幼儿请假消息数'
    ,parents_child_leave_num         int                  comment '家长幼儿请假消息数'
    ,inspection_num                  int                  comment '幼儿晨午检消息数'
    ,child_report_num                int                  comment '宝宝报告消息数'
    ,device_num                      int                  comment '智能设备管理消息数'
    ,teacher_incampus_num            int                  comment '教职工出勤消息数'
    ,user_child_medicine_num         int                  comment '用户幼儿喂药消息数'
    ,parents_child_medicine_num      int                  comment '家长幼儿喂药消息数'
    ,child_incampus_num              int                  comment '幼儿入园出勤消息数'
    ,electronic_fence_num            int                  comment '电子围栏消息数'
    ,parent_child_task_num           int                  comment '亲子任务消息数'
    ,push_all_num                    int                  comment '推送总人数'
    ,child_leave_num                 int                  comment '幼儿请假消息数'
    ,child_medicine_num              int                  comment '幼儿喂药消息数'
) comment '消息推送汇总表'
partitioned by (
    ,`part_date`                     string               comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;


=====================================================
create table if not exists dwh_dw_campus.clife_campus_dws_shit_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        tinyint              comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,student_cnt                     int                  comment '班级总人数'
    ,shit_num                        int                  comment '拉臭臭人数'
    ,no_shit_num                     int                  comment '未拉臭臭人数'
    ,into_campus_num                 int                  comment '入园人数'
    ,no_into_campus_num              int                  comment '未入园人数'
    ,date_time                       string               comment '测量时间'
) comment '班级拉臭臭汇总表'
partitioned by (
    ,`part_date`                     string               comment '跑数时间')
row format delimited fields terminated by '\t'
stored as parquet
;


========================================================
create table if not exists dwh_dw_campus.clife_campus_dws_intocampus_data(
     class_id                        bigint               comment '班级id'
    ,class_name                      string               comment '班级名称'
    ,grade_id                        bigint               comment '年级id'
    ,grade_name                      string               comment '年级名称'
    ,org_id                          bigint               comment '学校id'
    ,org_name                        string               comment '学校名称'
    ,area_id                         bigint               comment '学校区县id'
    ,student_cnt                     int                  comment '班级总学生人数'
    ,intocampus_num                  int                  comment '入园人数'
    ,no_intocampus_num               int                  comment '未入园人数'
    ,intocampus_rate                 double               comment '入园率'
    ,no_intocampus_rate              double               comment '未入园率'
    ,affair_leave_num                int                  comment '请事假人数'
    ,sick_leave_num                  int                  comment '请病假人数'
    ,leave_num                       int                  comment '请假人数'
    ,affair_leave_rate               double               comment '请事假人数占比'
    ,sick_leave_rate                 double               comment '请病假人数占比'
    ,data_time                       datetime             comment '数据日期'
)comment '入园汇聚表'
partitioned by (part_date string)
row format delimited fields terminated by '\t'
stored as parquet
;
