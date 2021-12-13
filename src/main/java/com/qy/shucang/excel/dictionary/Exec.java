package com.qy.shucang.excel.dictionary;

import com.qy.shucang.excel.createsql.MysqlTransformer;
import com.qy.shucang.excel.createsql.TransformEnum;
import com.qy.shucang.excel.format.CreateFormater;
import com.qy.shucang.excel.format.FormaterDDLEnum;
import com.qy.shucang.excel.format.FormaterDMLEnum;
import com.qy.shucang.excel.format.InsertFormater;

/**
 * 运行解析sql写入excel，生成数据字典。
 */
public class Exec {
    public static void main(String[] args) {
        new ExcelOdsCreateParser(DicEnum.ODS_DICTIONARY).exec();//生成ods数据字段
        new ExcelDwdCreateParser(DicEnum.DWD_DICTIONARY).exec();//生成dwd数据字段
        new ExcelDwsCreateParser(DicEnum.DWS_DICTIONARY).exec();//生成dws数据字段
        new ExcelDwsCreateParser(DicEnum.ADS_DICTIONARY).exec();//生成dws数据字段
        new ExcelDimCreateParser(DicEnum.DIM_DICTIONARY).exec();//生成dim数据字段
//        new InsertFormater(FormaterDMLEnum.ODS_DML_FORMATER).exec();//格式化ods select sql
//        new InsertFormater(FormaterDMLEnum.DWD_DML_FORMATER).exec();//格式化dwd insert sql
//        new InsertFormater(FormaterDMLEnum.DWS_DML_FORMATER).exec();//格式化dws insert sql
//        new InsertFormater(FormaterDMLEnum.DIM_DML_FORMATER).exec();//格式化dim insert sql
//        new CreateFormater(FormaterDDLEnum.ODS_DDL_FORMATER).exec();//格式化ods create sql
//        new CreateFormater(FormaterDDLEnum.DWD_DDL_FORMATER).exec();//格式化dwd create sql
//        new CreateFormater(FormaterDDLEnum.DWS_DDL_FORMATER).exec();//格式化dws create sql
//        new CreateFormater(FormaterDDLEnum.DIM_DDL_FORMATER).exec();//格式化dim create sql
//        new MysqlTransformer(TransformEnum.CAMPUS_MYSQL_INFO_TRANSFORM).exec();//校园建表
//        new MysqlTransformer(TransformEnum.HR_MYSQL_INFO_TRANSFORM).exec();//校园建表

    }
}
