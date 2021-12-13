package com.qy.shucang.excel.dictionary;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;

import org.apache.commons.lang.StringUtils;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 解析svn的create sql，同时寻找对应的insert/select sql。写入excel，具体运行任务选择其子类。
 */
public abstract class ExcelCreateParser implements ExcelParser{
    List<ExcelSheet<ExcelInfo>> sheetsData = new ArrayList<ExcelSheet<ExcelInfo>>();
    public String context;
    public static String errorMessage = "找不到sql，请手动添加";
    public ArrayList<Data> dataList = new ArrayList<Data>();
    public String sep = FileUtils.sep;
    public String type;
    public String database ;
    public PData pData;
    public DicEnum dicEnum;
    public void open() {
        this.type = dicEnum.getType();
        this.database = dicEnum.getDatabase();
        this.pData = dicEnum.getpData();
    }

    public void parser(){
        initContext();
        ProjectProcessJsonParser projectProcessJsonParser = new ProjectProcessJsonParser();
        projectProcessJsonParser.exec();
        dataList = projectProcessJsonParser.getDataList();

        parseText(sheetsData, context);
        deleteOldExcel(getWritePath());
        ExcelUtil.exportExcel(sheetsData, getWritePath());
    }
    private void parseText(List<ExcelSheet<ExcelInfo>> sheetsData, String text) {
        String[] arr = text.replaceAll("={3,100}.*","")
                .replaceAll("-{3,100}.*","")
                .split("create {1,100}table ");
        for (String sql : arr){
            if (!StringUtils.isBlank(sql)){
                ExcelSheet<ExcelInfo> infoExcelSheet = parseSql("create table " + sql);
                sheetsData.add(infoExcelSheet);
            }
        }
    }

    private ExcelSheet<ExcelInfo> parseSql(String sql) {
        ExcelSheet<ExcelInfo> sheetData = new ExcelSheet<ExcelInfo>();
        String[] headers = {"序号", "字段中文名称", "字段英文", "字段类型", "是否主键", "来源表", "来源逻辑", "建表逻辑", "备注" };

        ArrayList<ExcelInfo> infos = new ArrayList<ExcelInfo>();
        HiveStatementParser hsp = new HiveStatementParser(sql);
        SQLStatement sqlStatement = hsp.parseStatement();
        if (sqlStatement instanceof HiveCreateTableStatement){
            String fullTableName = ((HiveCreateTableStatement) sqlStatement).getTableSource().toString().replaceAll("`","");
            String databaseName = getDatabase(fullTableName.contains(".") ? fullTableName.split("\\.")[0] : "");
            String shortTableName = fullTableName.contains(".") ? fullTableName.split("\\.")[1] : fullTableName;
            SQLExpr tableComment = ((HiveCreateTableStatement) sqlStatement).getComment();
            String tc = tableComment == null ? "''" : tableComment.toString();
            tc= tc.substring(1,tc.length() -1);
            String sqlLine = "";
            List<SQLTableElement> tableElementList = ((HiveCreateTableStatement) sqlStatement).getTableElementList();
            int i = 0;
            for (; i < tableElementList.size(); i++) {
                SQLTableElement tableElement = tableElementList.get(i);
                if (tableElement instanceof SQLColumnDefinition){
                    ExcelInfo info;
                    SQLColumnDefinition te = (SQLColumnDefinition) tableElement;
                    String comment = te.getComment() == null ? "''" : te.getComment().toString().trim();
                    comment = comment.substring(1,comment.length() -1);
                    if (i == 0){
                        sqlLine = findSql(shortTableName);
                        String fromTable = findFromTableSql(sqlLine);
                        info = new ExcelInfo(i + 1 + "", comment, te.getName().toString().replaceAll("`",""),
                                te.getDataType().getName(), "",fromTable, sqlLine, sql, "");
                    } else {
                        info = new ExcelInfo(i + 1 + "", comment, te.getName().toString().replaceAll("`",""),
                                te.getDataType().getName(), "","", "", "", "");
                    }
                    infos.add(info);
                } else {
                    throw new RuntimeException("sql field type is not SQLColumnDefinition, please add some code for it");
                }
            }
            List<SQLColumnDefinition> partitionColumns = ((HiveCreateTableStatement) sqlStatement).getPartitionColumns();
            for (SQLColumnDefinition partition : partitionColumns){
                String comment = partition.getComment() == null ? "'分区字段'" : partition.getComment().toString().trim();
                comment = comment.substring(1,comment.length() -1);
                ExcelInfo info = new ExcelInfo(i + 1 + "", comment, partition.getName().toString().replaceAll("`",""),
                        partition.getDataType().getName(), "","", "","","");
                infos.add(info);

            }
            String sheetName = StringUtils.isBlank(tc) ? shortTableName : tc;
            if (sqlLine.equals(errorMessage)){
                sheetName += "_1";
            }
            if (StringUtils.isBlank(databaseName)){
                sheetName += "_2";
            }
            if (StringUtils.isBlank(tc)){
                sheetName += "_3";
            }
            sheetData.setDataset(infos);
            sheetData.setHeaders(headers);
            sheetData.setSheetName(sheetName.replaceAll("/","或"));
            sheetData.setRegionInfos(getRegionInfos(sheetData));
            sheetData.setClassName(ExcelInfo.class);
            sheetData.setFirstLine(new String[]{shortTableName, "", databaseName, "", tc, ""});
        } else {
            throw new RuntimeException("sql is not create sql, please check it");
        }
        return sheetData;
    }

    public abstract String findSql(String shortTableName);
    public abstract String findFromTableSql(String sqlLine);

    private void deleteOldExcel(String file) {
        File file1 = new File(file);
        if (file1.exists()){
            file1.delete();
            System.out.println("删除 " + file + " 成功");
        } else {
            System.out.println(file + " is not exists");
        }
    }
    public abstract String getDatabase(String database);
    public abstract String getReadPath();
    public abstract String getWritePath();
    public abstract void initContext();
    public ArrayList<RegionInfo> getRegionInfos(ExcelSheet<ExcelInfo> sheetData){
        ArrayList<RegionInfo> regionInfos = new ArrayList<RegionInfo>();
        regionInfos.add(new RegionInfo(2, Integer.MAX_VALUE, 6,6));
        regionInfos.add(new RegionInfo(2, Integer.MAX_VALUE, 7,7));
        regionInfos.add(new RegionInfo(0, 0, 0,1));
        regionInfos.add(new RegionInfo(0, 0, 2,3));
        regionInfos.add(new RegionInfo(0, 0, 4,6));
        if (!StringUtils.isBlank(sheetData.getDataset().iterator().next().s)){
            regionInfos.addAll(getOtherRegionInfos());
        }
        return regionInfos;
    }
    public abstract ArrayList<RegionInfo> getOtherRegionInfos();
    public void exec() {
        open();
        parser();
        close();
    }

    public void close() {
        System.out.println("task is success");
    }
}
