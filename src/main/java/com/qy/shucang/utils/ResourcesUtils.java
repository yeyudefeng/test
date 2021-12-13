package com.qy.shucang.utils;

public class ResourcesUtils {
    /**
     * 可以读，但是写的时候，写到target目录去了。读取也是target目录的编译文件。
     * /C:/Users/86000014/Desktop/bracelet/target/classes/replace/partdate/write_replace_part_date_sql.sql
     * @param path
     * @return
     */
    public static String getPath(String path){
        return ResourcesUtils.class.getClassLoader().getResource(path).getPath();
    }
}
