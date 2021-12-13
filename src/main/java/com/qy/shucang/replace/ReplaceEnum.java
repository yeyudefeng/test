package com.qy.shucang.replace;


public enum ReplaceEnum {
    /**
     * 读取文件路径
     */
    READ_PATH("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\replace\\partdate\\read_replace_part_date_sql.sql")
    ,

    /**
     * 写入文件路径
     */
    WRITE_PATH("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\replace\\partdate\\write_replace_part_date_sql.sql")
    ,

    /**
     * 分区字段正则表达式
     */
    PARTDATE_REGEXP("regexp_replace {0,10}\\( {0,10}date_sub {0,10}\\( {0,10}current_date {0,10}\\( {0,10}\\) {0,10}, {0,10}1 {0,10}\\) {0,10}, {0,10}'-' {0,10}, {0,10}'' {0,10}\\)")
    ,
    /**
     * system.biz.date
     */
    SYSTEM_BIZ_DATE("\\$\\{system.biz.date\\}")
    ,

    /**
     * system.biz.curdate
     */
    SYSTEM_BIZ_CURDATE("\\$\\{system.biz.curdate\\}")
    ,

    /**
     * system.biz.curdate
     */
    SYSTEM_DATETIME("\\$\\{system.datetime\\}")
    ;

    private String value;

    ReplaceEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
