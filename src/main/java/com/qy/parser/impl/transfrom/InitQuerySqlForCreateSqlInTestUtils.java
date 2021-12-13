package com.qy.parser.impl.transfrom;

import com.qy.parser.utils.FileUtils;
import com.qy.parser.utils.MysqlUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;

import static com.qy.parser.enums.EInfo.*;

public class InitQuerySqlForCreateSqlInTestUtils {

    public static void main(String[] args) {
        new InitQuerySqlForCreateSqlInTestUtils().exec();
    }
    public void exec(){
        queryTables();
    }
    private void queryTables() {

        Connection con = null;
        Statement statement= null;
        ResultSet resultSet = null;
        try {
            ArrayList<String> list = new ArrayList<>();
            con = DriverManager.getConnection("jdbc:mysql://10.6.14.1:3306/db_bigdataplatform?characterEncoding=UTF-8", "root", "123456");
            statement = con.createStatement();
            resultSet = statement.executeQuery("select distinct(tm_dw_name) from t_pt_table_manage where tm_name like '"+ CLIFE + "_"+SHUCANG_NAME+"_%'");
            System.out.println("select distinct(tm_dw_name) from t_pt_table_manage where tm_name like '"+ CLIFE + "_"+SHUCANG_NAME+"_%'");
            while (resultSet.next()){
                String fullTableName = resultSet.getString(1);
                list.add(fullTableName);
            }
            assert list.size() > 0;
            String tableStr = "";
            for (String table : list){
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
            ResultSet resultSet1 = statement.executeQuery(queryFieldsSql);
            StringBuffer sb1 = new StringBuffer();
            while (resultSet1.next()){
                sb1.append("\n").append(resultSet1.getInt(1));
                sb1.append(",").append(resultSet1.getString(2));
                sb1.append(",").append(resultSet1.getString(3));
                sb1.append(",").append(resultSet1.getInt(4));
                sb1.append(",").append(resultSet1.getString(5));
                sb1.append(",").append(resultSet1.getString(6));
                sb1.append(",").append(resultSet1.getString(7));
            }
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DW_FIELD_CSV, sb1.toString());
            String queryPartitionSql = "select\n" +
                    "a.tm_id, a.tm_dw_name, a.tm_describe,\n" +
                    "c.te_id, c.te_name, c.te_filed_type\n" +
                    "from (\n" +
                    "SELECT * FROM `t_pt_table_manage` where tm_dw_name in ( " + tableStr +  " )\n" +
                    ") a\n" +
                    "left join t_pt_table_expansion c\n" +
                    "on a.tm_id = c.tm_id where c.tm_id is not null";
            System.out.println("queryPartitionSql" + "\n" + queryPartitionSql);
            ResultSet resultSet2 = statement.executeQuery(queryPartitionSql);
            StringBuffer sb2 = new StringBuffer();
            while (resultSet2.next()){
                sb2.append("\n").append(resultSet2.getInt(1));
                sb2.append(",").append(resultSet2.getString(2));
                sb2.append(",").append(resultSet2.getString(3));
                sb2.append(",").append(resultSet2.getInt(4));
                sb2.append(",").append(resultSet2.getString(5));
                sb2.append(",").append(resultSet2.getString(6));
            }
            FileUtils.write(TRANSFORM_PATH_HIVE_CREATE_DW_PARTITION_CSV, sb2.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("query mysql table_manege catch some error");
        } finally {
            MysqlUtils.close(con, statement, resultSet);
        }

    }
}
