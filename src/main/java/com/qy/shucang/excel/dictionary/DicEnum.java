package com.qy.shucang.excel.dictionary;

import com.qy.parser.enums.EInfo;

import static com.qy.parser.enums.EInfo.TRANSFORM_PATH_HIVE_CREATE_ODS_PATH;

/**
 * 数据字典参数枚举
 */
public enum DicEnum {
//    ODS_DICTIONARY("ods","dwh_ods_campus",
//            new PData ("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\ods层sql\\ods建表.sql",
//            "D:\\QY\\文件\\校园数仓笔记\\数据字典\\ods数据字典.xlsx")
//    ),
//    DWD_DICTIONARY("dwd","dwh_dw_campus",
//            new PData ("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dwd建表\\dwd建表.sql",
//                    "D:\\QY\\文件\\校园数仓笔记\\数据字典\\dwd数据字典.xlsx")
//    ),
//    DWS_DICTIONARY("dws","dwh_dw_campus",
//            new PData ("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dws建表\\dws建表.sql",
//                    "D:\\QY\\文件\\校园数仓笔记\\数据字典\\dws数据字典.xlsx")
//    ),
//    DIM_DICTIONARY("dim","dwh_dw_campus",
//            new PData ("D:\\QY\\文件\\数仓\\校园数仓\\新建文件夹\\校园数仓\\技术文档\\sql\\dim建表\\dim建表.sql",
//            "D:\\QY\\文件\\校园数仓笔记\\数据字典\\dim数据字典.xlsx")
//    )


    ODS_DICTIONARY(EInfo.ODS,EInfo.DB + EInfo.UNDERLINE + EInfo.ODS + EInfo.SHUCANG_NAME,
            new PData (EInfo.TRANSFORM_PATH_HIVE_CREATE_ODS_PATH,
                    EInfo.SHUCANG_PATH + "\\parser\\excel\\ods数据字典.xlsx")
    ),
    DWD_DICTIONARY(EInfo.DWD,EInfo.DB + EInfo.UNDERLINE + EInfo.DWD + EInfo.SHUCANG_NAME,
            new PData (EInfo.TRANSFORM_PATH_HIVE_CREATE_DWD_PATH,
                    EInfo.SHUCANG_PATH + "\\parser\\excel\\dwd数据字典.xlsx")
    ),
    DWS_DICTIONARY(EInfo.DWS,EInfo.DB + EInfo.UNDERLINE + EInfo.DWS + EInfo.SHUCANG_NAME,
            new PData (EInfo.TRANSFORM_PATH_HIVE_CREATE_DWS_PATH,
                    EInfo.SHUCANG_PATH + "\\parser\\excel\\dws数据字典.xlsx")
    ),
    DIM_DICTIONARY(EInfo.DIM,EInfo.DB + EInfo.UNDERLINE + EInfo.DIM + EInfo.SHUCANG_NAME,
            new PData (EInfo.TRANSFORM_PATH_HIVE_CREATE_DIM_PATH,
                    EInfo.SHUCANG_PATH + "\\parser\\excel\\dim数据字典.xlsx")
    ),
    ADS_DICTIONARY(EInfo.ADS,EInfo.DB + EInfo.UNDERLINE + EInfo.ADS + EInfo.SHUCANG_NAME,
            new PData (EInfo.TRANSFORM_PATH_HIVE_CREATE_ADS_PATH,
                    EInfo.SHUCANG_PATH + "\\parser\\excel\\ads数据字典.xlsx")
    )
    ;

    DicEnum(String type, String database, PData pData) {
        this.type = type;
        this.database = database;
        this.pData = pData;
    }

    private String type;
    private String database;
    private PData pData;

    public String getType() {
        return type;
    }

    public String getDatabase() {
        return database;
    }

    public PData getpData() {
        return pData;
    }
}
