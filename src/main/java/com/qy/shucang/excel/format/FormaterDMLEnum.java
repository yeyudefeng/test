package com.qy.shucang.excel.format;

/**
 * 格式化参数枚举
 */
public enum FormaterDMLEnum {

    ODS_DML_FORMATER("dwh_campus_ods.txt", "dwh_campus_ods.txt"),
    DWD_DML_FORMATER("dwh_campus_dwd.txt", "dwh_campus_dwd.txt"),
    DWS_DML_FORMATER("dwh_campus_dws.txt", "dwh_campus_dws.txt"),
    DIM_DML_FORMATER("dwh_campus_dim.txt", "dwh_campus_dim.txt")
    ;
    private final String readPathPrefix = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\insertsql\\";
    private final String writePathPrefix = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\formatdmlsql\\";
    private final String readPath;
    private final String writePath;

    FormaterDMLEnum(String readPath, String writePath) {
        this.readPath = readPath;
        this.writePath = writePath;
    }

    public String getReadPath() {
        return readPathPrefix + readPath;
    }

    public String getWritePath() {
        return writePathPrefix + writePath;
    }
}


