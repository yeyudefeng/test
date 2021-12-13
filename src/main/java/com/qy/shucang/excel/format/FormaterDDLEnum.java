package com.qy.shucang.excel.format;

import com.qy.shucang.excel.dictionary.PData;

public enum FormaterDDLEnum {
    ODS_DDL_FORMATER("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\ods层sql\\ods建表.sql",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\formatddlsql\\ods建表.sql"
    ),
    DWD_DDL_FORMATER("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dwd建表\\dwd建表.sql",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\formatddlsql\\dwd建表.sql"
    ),
    DWS_DDL_FORMATER("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dws建表\\dws建表.sql",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\formatddlsql\\dws建表.sql"
    ),
    DIM_DDL_FORMATER("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dim建表\\dim建表.sql",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\formatddlsql\\dim建表.sql"
    )
    ;
    private final String readPath;
    private final String writePath;
    FormaterDDLEnum(String readPath, String writePath) {
        this.readPath = readPath;
        this.writePath = writePath;
    }

    public String getReadPath() {
        return  readPath;
    }

    public String getWritePath() {
        return  writePath;
    }
}
