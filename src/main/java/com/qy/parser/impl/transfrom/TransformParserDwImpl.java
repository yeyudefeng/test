package com.qy.parser.impl.transfrom;

import com.google.inject.internal.asm.$ClassAdapter;
import com.qy.parser.bean.EDT;
import com.qy.parser.bean.EField;
import com.qy.parser.bean.EProcessData;
import com.qy.parser.interfaces.TransformParser;
import com.qy.parser.utils.FileUtils;
import com.qy.parser.utils.KeyWordUtils;
import com.qy.parser.utils.MysqlUtils;
import com.qy.parser.utils.TypeTransformUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static com.qy.parser.enums.EInfo.*;

/**
 * 查询生产数据库的sql，生成对应的csv文件
 * select
 * a.tm_id, a.tm_dw_name, a.tm_describe,
 * b.ts_id, b.ts_name,b.ts_filed_type ,b.ts_filed_describe
 * from (
 * SELECT * FROM `t_pt_table_manage` where tm_dw_name in ('db_dw_hr.clife_hr_dws_market_by_product_id_d','db_dw_hr.clife_hr_dws_market_by_product_id_y')
 * ) a
 * left join t_pt_table_structure b
 * on a.tm_id = b.tm_id
 *
 *
 *
 *
 * select
 * a.tm_id, a.tm_dw_name, a.tm_describe,
 * c.te_id, c.te_name, c.te_filed_type
 * from (
 * SELECT * FROM `t_pt_table_manage` where tm_dw_name in ('db_dw_hr.clife_hr_dws_market_by_product_id_d','db_dw_hr.clife_hr_dws_market_by_product_id_y')
 * ) a
 * left join t_pt_table_expansion c
 * on a.tm_id = c.tm_id
 */

public class TransformParserDwImpl implements TransformParser {
    public HashMap<Integer, EDT> tables = new HashMap<>();
    public HashMap<Integer, ArrayList<EField>> fields= new HashMap<>();
    public HashMap<Integer, ArrayList<EField>> partitions= new HashMap<>();
    private HashSet<String> mysqlKeyWordSet = KeyWordUtils.getKeyWords(KEYWORDS_PATH_MYSQL);
    private StringBuffer hiveCreateSqlSB = new StringBuffer();
    private StringBuffer hiveDWDCreateSqlSB = new StringBuffer();
    private StringBuffer hiveDWSCreateSqlSB = new StringBuffer();
    private StringBuffer hiveADSCreateSqlSB = new StringBuffer();
    private StringBuffer hiveDIMCreateSqlSB = new StringBuffer();
    private StringBuffer hiveODSCreateSqlSB = new StringBuffer();
    private static Integer len = 30;
    @Override
    public void transform() {
        parseHiveCreateSql();
    }

    @Override
    public void open() {
        System.out.println("start ");
        parseFields();
        parsePartitions();
        for (Integer tmId : tables.keySet()){
            EDT edt = tables.get(tmId);
            edt.fields = fields.get(tmId);
            edt.partitions = partitions.getOrDefault(tmId, new ArrayList<>());
        }
    }


    private void parseHiveCreateSql() {
        for (EDT dt : tables.values()){
            String cs = "create table if not exists " + dt.database + "." + dt.tablename + " ( "+ FILE_SEP;
            for (EField field : dt.fields){
                cs += "    ," + StringUtils.rightPad(transformField(field.l), len, " ") + " " + StringUtils.rightPad(TypeTransformUtils.transfromType(field.m), len - 10 , " ");
                if (!StringUtils.isBlank(field.r)){
                    cs += " comment '" + field.r + "' ";
                }
                cs += FILE_SEP;
            }
            cs += ") ";
            if (!StringUtils.isBlank(dt.tableComment) && !dt.tableComment.equalsIgnoreCase("null")){
                cs += "comment '" + dt.tableComment + "' ";
            }

            String partitionLine = "";
            for (EField partition : dt.partitions){
                partitionLine += ", " + partition.l + " " + partition.m + " ";
            }
            partitionLine = partitionLine.replaceFirst(",","");
            if (!StringUtils.isBlank(partitionLine)){
                cs += FILE_SEP;
                cs += "partitioned by ( " + partitionLine + " ) ";
            }
            cs += FILE_SEP +
                    "row format delimited fields terminated by '\\t' " + FILE_SEP +
                    "stored as parquet " + FILE_SEP + ";";
            cs = cs.replaceFirst(",", " ");
            dt.sql = cs;
            insertEachSqlToFile(dt);
            hiveCreateSqlSB.append(format(dt, dt.sql));
        }

        if (IS_WRITE_TRANSFORM_JSON){
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DW_SQL, hiveCreateSqlSB.toString());
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_ODS_PATH , hiveODSCreateSqlSB.toString());
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DWD_PATH, hiveDWDCreateSqlSB.toString());
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DWS_PATH, hiveDWSCreateSqlSB.toString());
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_ADS_PATH, hiveADSCreateSqlSB.toString());
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DIM_PATH, hiveDIMCreateSqlSB.toString());
        }
    }

    private void insertEachSqlToFile(EDT edt) {
        String sql = edt.sql;
        String fileName = "";
        String level = "";
        String tableName = edt.tablename;
        if (tableName.contains("dim")){
            level = DIM;
            hiveDIMCreateSqlSB.append(format(edt, edt.sql));
        } else if (tableName.contains("ods")){
            level = ODS;
            hiveODSCreateSqlSB.append(format(edt, edt.sql));
        } else if (tableName.contains("dwd")){
            level = DWD;
            hiveDWDCreateSqlSB.append(format(edt, edt.sql));
        } else if (tableName.contains("dws")){
            level = DWS;
            hiveDWSCreateSqlSB.append(format(edt, edt.sql));
        } else if (tableName.contains("ads")){
            level = ADS;
            hiveADSCreateSqlSB.append(format(edt, edt.sql));
        } else {
            throw new RuntimeException( " can not match level");
        }

//        fileName = SHUCANG_PATH + "\\parser\\separate\\" + SHUCANG_NAME + UNDERLINE + level + UNDERLINE + "建表";
        fileName = SHUCANG_PATH + "\\parser\\separate\\" + SHUCANG_NAME + BACKSLASH + level + BACKSLASH + "建表";
        File file = new File(fileName);
        if (!file.exists()){
            file.mkdirs();
        }
        FileUtils.write(file.getAbsolutePath() + "\\" + tableName + ".sql" , sql);
    }
    private StringBuffer format(EDT dt, String sql){
        return new StringBuffer().append(StringUtils.rightPad("", MAX_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad(StringUtils.rightPad("", MIN_LEN, "=") + StringUtils.rightPad("", MIN_LEN, " ") + dt.database + "." + dt.tablename, MAX_LEN - MIN_LEN, " ") + StringUtils.rightPad("", MIN_LEN, "=") + FILE_SEP)
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

    private void parsePartitions() {
        String context = FileUtils.read(TRANSFORM_PATH_HIVE_CREATE_DW_PARTITION_CSV);
        String[] arr = context.split(FILE_SEP);
        Boolean isFirst = true;
        for (String line : arr){
            if (isFirst){
                isFirst = false;
                continue;
            }
            String[] arr1 = line.split(",");
            int tmId = Integer.parseInt(arr1[0]);
            ArrayList<EField> partitionList = partitions.getOrDefault(tmId, new ArrayList<EField>());
            partitionList.add(new EField(arr1[4], arr1[5].toLowerCase(), null));
            partitions.put(tmId, partitionList);
        }
    }

    private void parseFields() {
        String context = FileUtils.read(TRANSFORM_PATH_HIVE_CREATE_DW_FIELD_CSV);
        String[] arr = context.split(FILE_SEP);
        Boolean isFirst = true;
        for (String line : arr){
            if (isFirst){
                isFirst = false;
                continue;
            }
            String[] arr1 = line.split(",");
            int tmId = Integer.parseInt(arr1[0]);
            String database = arr1[1].split("\\.")[0];
            String tableName = arr1[1].split("\\.")[1];
            tables.put(tmId, new EDT(database, tableName, arr1[2]));
            ArrayList<EField> fieldList = fields.getOrDefault(tmId, new ArrayList<EField>());
            fieldList.add(new EField(arr1[4], arr1[5].toLowerCase(), arr1[6]));
            fields.put(tmId, fieldList);
        }
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


    private void initQuerySql(){
        ArrayList<String> tableList = queryTables();
        // TODO: 2021/11/5 赋值给tableList
        assert tableList.size() > 0;
        String tableStr = "";
        for (String table : tableList){
            tableStr += "," + "'" + table + "'";
        }
        tableStr = tableStr.replaceFirst(",","");
        String queryFieldsSql = "select \n" +
                "a.tm_id, a.tm_dw_name, a.tm_describe, \n" +
                "c.ts_id, c.ts_name, c.ts_filed_type, c.ts_filed_describe \n" +
                "from (\n" +
                "SELECT * FROM `t_pt_table_manage` where tm_dw_name in ( " + tableStr +  " )\n" +
                ") a \n" +
                "left join t_pt_table_structure c \n" +
                "on a.tm_id = c.tm_id ";
        System.out.println("queryFieldsSql" + "\n" + queryFieldsSql);
        String queryPartitionSql = "select\n" +
                "a.tm_id, a.tm_dw_name, a.tm_describe,\n" +
                "c.te_id, c.te_name, c.te_filed_type\n" +
                "from (\n" +
                "SELECT * FROM `t_pt_table_manage` where tm_dw_name in ( " + tableStr +  " )\n" +
                ") a\n" +
                "left join t_pt_table_expansion c\n" +
                "on a.tm_id = c.tm_id ";
        System.out.println("queryPartitionSql" + "\n" + queryPartitionSql);
    }

    private ArrayList<String> queryTables() {
        ArrayList<String> list = new ArrayList<>();
        Connection con = null;
        Statement statement= null;
        ResultSet resultSet = null;
        try {
            con = DriverManager.getConnection("jdbc:mysql://10.6.14.1:3306/bigdataplatform?characterEncoding=UTF-8", "root", "123456");
            statement = con.createStatement();
            resultSet = statement.executeQuery(String.format("select distinct(tm_dw_name) from t_pt_table_manage where tm_name like '%s_%s_%' ", CLIFE, SHUCANG_NAME));
            System.out.println(String.format("select distinct(tm_dw_name) from t_pt_table_manage where tm_name like '%s_%s_%' ", CLIFE, SHUCANG_NAME));
            while (resultSet.next()){
                String fullTableName = resultSet.getString(1);
                list.add(fullTableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("query mysql table_manege catch some error");
        } finally {
            MysqlUtils.close(con, statement, resultSet);
        }

        return list;
    }
}
