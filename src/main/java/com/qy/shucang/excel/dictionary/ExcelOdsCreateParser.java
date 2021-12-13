package com.qy.shucang.excel.dictionary;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

public class ExcelOdsCreateParser extends ExcelCreateParser {
    private static String tablePrefix = "clife_nstc_ods";

    public ExcelOdsCreateParser (DicEnum dicEnum){
        this.dicEnum = dicEnum;
    }

    public String findSql(String shortTableName) {
        for (Data data : dataList){
            if (data.processName.contains("ods")){
                String tableName = data.name.contains(".") ? data.name.split("\\.")[1] : data.name;
//                tableName = tablePrefix + tableName.substring(2);
                if (tableName.equalsIgnoreCase(shortTableName)){
                    return data.sql;
                }
            }
        }
        System.err.println("mysql表名和ods表名，不能按照既定规则匹配。也有可能是建表而没有对应的表工作刘task，匹配不上导致，请检查工作流的task名称是否为mysql名称，ods建表名称是否符合既定规范，dwd，dws的表名映射是否规范：" + shortTableName);
        return errorMessage;
    }

    public String findFromTableSql(String sqlLine) {
        if (!sqlLine.equals(errorMessage)){
            return sqlLine.split("from ")[1].trim().split(" {1,100}")[0].split(";")[0].split("\n")[0];
        }
        return "";
    }

    public String getDatabase(String database) {
        return StringUtils.isBlank(database) ? this.database : database;
    }

    public String getReadPath() {
        return pData.readPath;
    }

    public String getWritePath() {
        return pData.writePath;
    }

    public void initContext() {
        context = FileUtils.read(getReadPath()).toLowerCase();
    }

    public ArrayList<RegionInfo> getOtherRegionInfos() {
        ArrayList<RegionInfo> regionInfos = new ArrayList<RegionInfo>();
        regionInfos.add(new RegionInfo(2, Integer.MAX_VALUE, 5,5));
        return regionInfos;
    }


}
