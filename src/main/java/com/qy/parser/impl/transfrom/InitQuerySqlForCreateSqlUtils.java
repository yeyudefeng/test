package com.qy.parser.impl.transfrom;

import com.qy.parser.enums.EInfo;
import com.qy.parser.utils.FileUtils;
import org.apache.commons.lang.StringUtils;

import static com.qy.parser.enums.EInfo.*;

public class InitQuerySqlForCreateSqlUtils {


    public static void init(){

    }

    private static void getOdsTableNames(){
        String context = FileUtils.read(TRANSFORM_PATH_TABLES);
        String[] arr = context.split(FILE_SEP);
        for(String line : arr){
            if (!StringUtils.isBlank(line)){
                String odsTableName = COMMON_ODS_DATABASE + "." + COMMON_HIVE_ODS_TABLE_PREFIX + line.split("\\.")[1].substring(3);
            }
        }
    }
}
