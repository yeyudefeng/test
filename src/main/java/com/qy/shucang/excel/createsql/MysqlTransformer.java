package com.qy.shucang.excel.createsql;

import com.qy.shucang.excel.dictionary.FileUtils;

import com.qy.shucang.excel.format.Field;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * 生成半自动化的hive建表语句和hive ods select语句，
 * 需要自动手动选择ods建表是否分区，ods select是否添加where条件
 */
public class MysqlTransformer implements Transformer {
    private String createSql = "create table if not exists ";
    private static Integer len = 30;
    private String sep = FileUtils.sep;
    private String hiveDatabase = "dwh_ods_campus";
    private String hiveOdsTablePrefix = "clife_campus_ods_";
    private String jdbc;
    private String username;
    private String password;
    private String readPath;
    private String mysqlCreateSqlPath;
    private String hiveCreateSqlPath;
    private String hiveSelectSqlPath;
    private ArrayList<DT> list = new ArrayList<>();
    private ArrayList<DT> dts = new ArrayList<>();
    private ArrayList<String> hiveCreateSqls = new ArrayList<>();
    private ArrayList<String> hiveSelectSqls = new ArrayList<>();
    private StringBuffer mysqlCreateSqlSB = new StringBuffer();
    private StringBuffer hiveCreateSqlSB = new StringBuffer();
    private StringBuffer hiveSelectSqlSB = new StringBuffer();
    private TransformEnum transformEnum;
    public void open() {
        jdbc = transformEnum.getJdbc();
        username = transformEnum.getUsername();
        password = transformEnum.getPassword();
        readPath = transformEnum.getpData().readPath;
        mysqlCreateSqlPath = transformEnum.getpData().writePath;
        hiveCreateSqlPath = transformEnum.getpData2().readPath;
        hiveSelectSqlPath = transformEnum.getpData2().writePath;

        initMap();
        dts = MysqlUtils.queryCreateSqlFromMysql(jdbc, username, password, list, mysqlCreateSqlPath);

    }

    private void writeMysqlCreateSql() {
        FileUtils.write(mysqlCreateSqlPath, mysqlCreateSqlSB.toString());
    }
    private void writeHiveCreateSql() {
        FileUtils.write(hiveCreateSqlPath, hiveCreateSqlSB.toString());
    }
    private void writeHiveSelectSql() {
        FileUtils.write(hiveSelectSqlPath, hiveSelectSqlSB.toString());
    }
    private void initHiveCreateSql() {
        for (DT dt : dts){
            String cs = createSql + hiveDatabase + "." + hiveOdsTablePrefix + dt.tablename.substring(3) + " ( "+ sep;
            for (Field field : dt.fields){
                cs += "    ," + StringUtils.rightPad(field.l, len, " ") + " " + StringUtils.rightPad(transfromType(field.m), len - 10 , " ");
                if (!StringUtils.isBlank(field.r)){
                    cs += " comment '" + field.r + "' ";
                }
                cs += sep;
            }
            cs += ") ";
            if (!StringUtils.isBlank(dt.tableComment)){
                cs += "comment '" + dt.tableComment + "' ";
            }
            cs += sep;
            cs += "partitioned by ( part_date string ) " + sep +
                    "row format delimited fields terminated by '\\t' " + sep +
                    "stored as parquet " + sep + ";";
            cs = cs.replaceFirst(",", " ");
            hiveCreateSqls.add(cs);
            hiveCreateSqlSB.append("==================").append(sep)
                    .append(dt.database + "." + dt.tablename).append(sep)
                    .append("==================").append(sep)
                    .append(cs).append(sep).append(sep).append(sep);
        }
    }
    private void initHiveSelectSql() {
        for (DT dt : dts){
            String sql = "select " + sep;
            for (Field field : dt.fields){
                sql += "    ," + StringUtils.rightPad(field.l, len, " ");
                if (!StringUtils.isBlank(field.r)){
                    sql += " -- " + field.r ;
                }
                sql += sep;
            }
            sql += "from " + dt.database + "." + dt.tablename;
            sql.replaceFirst(","," ");
            hiveSelectSqls.add(sql);
            hiveSelectSqlSB.append("==================").append(sep)
                    .append(dt.database + "." + dt.tablename).append(sep)
                    .append("==================").append(sep)
                    .append(sql).append(sep).append(sep).append(sep);
        }

    }
    private void initMysqlCreateSql() {
        for (DT dt : dts){
            mysqlCreateSqlSB.append("==================").append(sep)
                    .append(dt.database + "." + dt.tablename).append(sep)
                    .append("==================").append(sep)
                    .append(dt.sql).append(sep).append(sep).append(sep);
        }
    }

    public MysqlTransformer(TransformEnum transformEnum) {
        this.transformEnum = transformEnum;
    }

    public void transformer() {
        initMysqlCreateSql();
        writeMysqlCreateSql();
        initHiveCreateSql();
        writeHiveCreateSql();
        initHiveSelectSql();
        writeHiveSelectSql();
    }

    private String transfromType(String type) {
        if (StringUtils.isBlank(type)){
            throw new RuntimeException("type is null");
        }
        if (type.contains("varchar") || type.contains("char") || type.contains("text")){
            return "string";
        }
        if (type.contains("bigint")){
            return "bigint";
        }
        if (type.contains("tinyint")){
            return "tinyint";
        }
        if (type.contains("float") || type.contains("decimal") || type.contains("double")){
            return "double";
        }
        if (type.contains("int")){
            return "int";
        }
        if (type.contains("timestamp")){
            return "timestamp";
        }
        if (type.contains("date")){
            return "date";
        }
        if (type.contains("datetime") || type.contains("time")){
            return "datetime";
        }
        throw new RuntimeException("type " + type + " is unknown");
    }

    private void initMap(){
        String context = FileUtils.read(readPath);
        String[] arr = context.split(sep);
        HashSet<String> hashSet = new HashSet<>();
        for (String line : arr){
            if (!StringUtils.isBlank(line) && !hashSet.contains(line) && !line.trim().startsWith("#")){
                hashSet.add(line);
                String[] ss = line.split("\\.");
                String database = ss[0];
                String tablename = ss[1];
                if (ss.length == 2 && !StringUtils.isBlank(database) && !StringUtils.isBlank(tablename) ){
                    list.add(new DT(database, tablename));
                }else {
                    throw new RuntimeException("only contains tablename , no database name or not right : " + line);
                }
            }
        }
        Iterator<String> iterator = hashSet.iterator();
        while (iterator.hasNext()){
            String line = iterator.next();

        }
    }
    public void exec() {
        open();
        transformer();
        close();
    }

    public void close() {
        System.out.println("task is success");
    }
}
