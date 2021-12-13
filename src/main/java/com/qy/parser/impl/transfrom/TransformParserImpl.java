package com.qy.parser.impl.transfrom;

import com.qy.gmsz.GMZSUtils;
import com.qy.parser.bean.EDT;
import com.qy.parser.bean.EField;
import com.qy.parser.enums.EInfo;
import com.qy.parser.interfaces.TransformParser;

import com.qy.parser.utils.FileUtils;
import com.qy.parser.utils.KeyWordUtils;
import com.qy.parser.utils.MysqlUtils;

import com.qy.parser.utils.TypeTransformUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

import static com.qy.parser.enums.EInfo.*;

/**
 * 通过给出的mysql表，生成mysql建表语句，hive ods建表语句，datax select语句
 */

public class TransformParserImpl implements TransformParser {
    private String createSql = "create table if not exists ";
    private static Integer len = 30;
    private String hiveDatabase = COMMON_ODS_DATABASE;
    private String hiveOdsTablePrefix = COMMON_HIVE_ODS_TABLE_PREFIX;
    private String jdbc;
    private String username;
    private String password;
    private String readPath;
    private String mysqlCreateSqlPath;
    private ArrayList<EDT> list = new ArrayList<>();
    private ArrayList<EDT> dts = new ArrayList<>();
    private ArrayList<String> hiveCreateSqls = new ArrayList<>();
    private ArrayList<String> hiveSelectSqls = new ArrayList<>();
    private StringBuffer mysqlCreateSqlSB = new StringBuffer();
    private StringBuffer hiveCreateSqlSB = new StringBuffer();
    private StringBuffer hiveSelectSqlSB = new StringBuffer();
    private HashSet<String> mysqlKeyWordSet = KeyWordUtils.getKeyWords(KEYWORDS_PATH_MYSQL);
    private HashSet<String> hiveKeyWordSet = KeyWordUtils.getKeyWords(KEYWORDS_PATH_HIVE);
    private HashSet<String> noTimeTableSet = new HashSet<>();
    @Override
    public void transform() {
        parseMysqlCreateSql();
        parseHiveCreateSql();
        parseHiveSelectSql();
    }

    @Override
    public void open() {
        System.out.println("start ");
        jdbc = EInfo.TRANSFORM_MYSQL_JDBC;
        username = EInfo.TRANSFORM_MYSQL_USERNAME;
        password = EInfo.TRANSFORM_MYSQL_PASSWORD;
        readPath = EInfo.TRANSFORM_PATH_TABLES;
        mysqlCreateSqlPath = EInfo.TRANSFORM_PATH_MYSQL_CREATE_SQL;

        initMap();
        dts = MysqlUtils.queryCreateSqlFromMysql(jdbc, username, password, list, mysqlCreateSqlPath);
    }

    @Override
    public void exec() {
        open();
        transform();
        close();
    }

    @Override
    public void close() {
        System.out.println("end ");
    }

    private void parseHiveSelectSql() {
        for (EDT dt : dts){
            HashSet<String> createTimeFieldList = new HashSet<>();
            HashSet<String> updateTimeFieldList = new HashSet<>();
            String sql = "select " + FILE_SEP;
            for (EField field : dt.fields){
                sql += "    ," + StringUtils.rightPad(transformField(field.l), len, " ");
                if (!StringUtils.isBlank(field.r)){
                    sql += " -- " + field.r ;
                }
                sql += FILE_SEP;
                GMZSUtils.addTimeFields(createTimeFieldList, updateTimeFieldList, field);
            }

            String where = GMZSUtils.getWhere(createTimeFieldList, updateTimeFieldList);
            sql += "from " + dt.database + "." + dt.tablename;
            sql += where;
            sql.replaceFirst(","," ");
            if (StringUtils.isBlank(where) && !noTimeTableSet.contains(dt.database + "." + dt.tablename)){
                throw new RuntimeException("-- ** 温馨提示：this table has no suitable time field , please check. \n" +
                        "if it true, please add this table " + dt.database + "." + dt.tablename + " \ninto file " + TRANSFORM_PATH_NO_TIME_TABLES);
            }
            sql = sql.replaceFirst(","," ");
            hiveSelectSqls.add(sql);
            hiveSelectSqlSB.append(format(dt, sql));
        }
        if (IS_WRITE_TRANSFORM_JSON){
            FileUtils.write(TRANSFORM_PATH_HIVE_SELECT_SQL, hiveSelectSqlSB.toString());
        }

    }

    private void parseHiveCreateSql() {
        for (EDT dt : dts){
            String cs = createSql + hiveDatabase + "." + hiveOdsTablePrefix + dt.tablename.substring(3) + " ( "+ FILE_SEP;
            for (EField field : dt.fields){
                cs += "    ," + StringUtils.rightPad(transformField(field.l), len, " ") + " " + StringUtils.rightPad(TypeTransformUtils.transfromType(field.m), len - 10 , " ");
                if (!StringUtils.isBlank(field.r)){
                    cs += " comment '" + field.r + "' ";
                }
                cs += FILE_SEP;
            }
            cs += ") ";
            if (!StringUtils.isBlank(dt.tableComment)){
                cs += "comment '" + dt.tableComment + "' ";
            }
            cs += FILE_SEP;
            cs += "partitioned by ( part_date string ) " + FILE_SEP +
                    "row format delimited fields terminated by '\\t' " + FILE_SEP +
                    "stored as parquet " + FILE_SEP + ";";
            cs = cs.replaceFirst(",", " ");
            hiveCreateSqls.add(cs);
            hiveCreateSqlSB.append(format(dt, cs));
        }
        if (IS_WRITE_TRANSFORM_JSON){
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_ODS_SQL, hiveCreateSqlSB.toString());
        }
    }

    private void parseMysqlCreateSql() {
        for (EDT dt : dts){
            mysqlCreateSqlSB.append(format(dt, dt.sql));
        }
        if (IS_WRITE_TRANSFORM_JSON){
            FileUtils.write(mysqlCreateSqlPath, mysqlCreateSqlSB.toString());
        }
    }

    private StringBuffer format(EDT dt, String sql){
        return new StringBuffer().append(StringUtils.rightPad("", MAX_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad(StringUtils.rightPad("", MIN_LEN, "=") + StringUtils.rightPad("", MIN_LEN, " ") + dt.database + "." + dt.tablename, MAX_LEN - MIN_LEN, " ") + StringUtils.rightPad("", MIN_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad(StringUtils.rightPad("", MIN_LEN, "=") + StringUtils.rightPad("", MIN_LEN, " ") + hiveDatabase + "." + hiveOdsTablePrefix + dt.tablename.substring(3), MAX_LEN - MIN_LEN, " ") + StringUtils.rightPad("", MIN_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad("", MAX_LEN, "=") + FILE_SEP)
                .append(sql + FILE_SEP + FILE_SEP + FILE_SEP)
                ;
    }

    private String transformField(String field){
        if (KeyWordUtils.isKeyWord(mysqlKeyWordSet, field)){
            return "`" + field + "`";
        }
        return field;
    }

    private void initMap(){
        String context = FileUtils.read(readPath);
        String[] arr = context.split(FILE_SEP);
        HashSet<String> hashSet = new HashSet<>();
        for (String line : arr){
            if (!StringUtils.isBlank(line) && !hashSet.contains(line) && !line.trim().startsWith("#")){
                hashSet.add(line);
                String[] ss = line.split("\\.");
                String database = ss[0];
                String tablename = ss[1];
                if (ss.length == 2 && !StringUtils.isBlank(database) && !StringUtils.isBlank(tablename) ){
                    list.add(new EDT(database, tablename));
                }else {
                    throw new RuntimeException("only contains tablename , no database name or not right : " + line);
                }
            }
        }

        String context2 = FileUtils.read(TRANSFORM_PATH_NO_TIME_TABLES);
        noTimeTableSet.addAll(Arrays.stream(context2.split(FILE_SEP)).filter(StringUtils::isNotBlank).collect(Collectors.toList()));
    }
}
