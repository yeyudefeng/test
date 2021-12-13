package com.qy.gmsz;

import com.qy.parser.bean.EField;

import java.util.HashSet;

/**
 * 苗木数仓特有解析逻辑
 */
public class GMZSUtils {
    /**
     * 判断字段是否包含时间字段
     * @param createTimeFieldList
     * @param updateTimeFieldList
     * @param field
     */
    public static void addTimeFields(HashSet<String> createTimeFieldList, HashSet<String> updateTimeFieldList, EField field) {
        if (field.l.toLowerCase().startsWith("update")){
            updateTimeFieldList.add(field.l);
        } else if (field.l.toLowerCase().startsWith("create")){
            createTimeFieldList.add(field.l);
        }
    }

    /**
     * 根据对应的时间字段，拼接对应的where条件返回
     * @param createTimeFieldList
     * @param updateTimeFieldList
     * @return
     */
    public static String getWhere(HashSet<String> createTimeFieldList, HashSet<String> updateTimeFieldList) {
        if (createTimeFieldList.contains("create_time") && updateTimeFieldList.contains("update_time")){
            return "\nwhere date_format(if(update_time is null ,create_time,update_time),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') ";
        } else if (createTimeFieldList.contains("create_time") && !updateTimeFieldList.contains("update_time")){
            return "\nwhere date_format(create_time,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')";
        } else if (!createTimeFieldList.contains("create_time") && updateTimeFieldList.contains("update_time")){
            return "\nwhere date_format(update_time,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')";
        } else if (createTimeFieldList.contains("create_at") && updateTimeFieldList.contains("update_at")){
            return "\nwhere date_format(if(update_at is null ,create_at,update_at),'%Y-%m-%d') < date_format(now(),'%Y-%m-%d') ";
        } else if (createTimeFieldList.contains("create_at") && !updateTimeFieldList.contains("update_at")){
            return "\nwhere date_format(create_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')";
        } else if (!createTimeFieldList.contains("create_at") && updateTimeFieldList.contains("update_at")){
            return "\nwhere date_format(update_at,'%Y-%m-%d') < date_format(now(),'%Y-%m-%d')";
        } else {
            return "";
        }
    }
}
