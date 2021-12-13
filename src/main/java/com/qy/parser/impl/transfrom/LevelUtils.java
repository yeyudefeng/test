package com.qy.parser.impl.transfrom;

import static com.qy.parser.enums.EInfo.*;
import static com.qy.parser.enums.EInfo.ADS;

public class LevelUtils {
    public static String getLevel(String tableName){
        String level = null;
        if (tableName.contains("dim")){
            level = DIM;
        } else if (tableName.contains("dwd")){
            level = DWD;
        } else if (tableName.contains("dws")){
            level = DWS;
        } else if (tableName.contains("ads")){
            level = ADS;
        } else {
            throw new RuntimeException( " can not match level");
        }
        return level;
    }
}
