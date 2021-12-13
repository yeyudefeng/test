package com.qy.shucang.excel.createsql;

import com.qy.shucang.excel.format.Field;

import java.sql.*;
import java.util.ArrayList;

/**
 * 查询内网mysql表schema专用
 */
public class MysqlUtils {
    private static String useSql = "use %s";
    private static String showCreateSql = "show create table %s.%s";
    private static String selectComment = "SELECT TABLE_NAME,TABLE_COMMENT FROM information_schema.TABLES " +
            "WHERE table_schema='%s' and TABLE_NAME = '%s'";
    private static String selectFields = "SELECT COLUMN_NAME ,  DATA_TYPE ,  COLUMN_COMMENT  FROM  INFORMATION_SCHEMA.COLUMNS " +
            "where  table_schema ='%s'  AND  table_name  = '%s' order by ORDINAL_POSITION";
    public static ArrayList<DT> queryCreateSqlFromMysql(String jdbc, String username, String password, ArrayList<DT> list, String writePath){
        ArrayList<DT> tables = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(jdbc, username, password);
            for (DT dt : list) {
                statement = connection.createStatement();
                statement.execute(String.format(useSql, dt.database));
                ResultSet resultSet = statement.executeQuery(String.format(showCreateSql, dt.database, dt.tablename));
                String createSql = null;
                while (resultSet.next()){
                    createSql = resultSet.getString(2);
                }
                ResultSet rs = statement.executeQuery(String.format(selectFields, dt.database, dt.tablename));
                ArrayList<Field> fields = new ArrayList<>();
                while (rs.next()){
                    String field = rs.getString(1);
                    String type = rs.getString(2);
                    String comment = rs.getString(3);
                    fields.add(new Field(field, type, comment));
                }
                ResultSet rs2 = statement.executeQuery(String.format(selectComment, dt.database, dt.tablename));
                String tableComment = null;
                while (rs2.next()){
                    tableComment = rs2.getString(2);
                }
                tables.add(new DT(dt.database, dt.tablename, createSql, fields, tableComment));
                close(null, null, resultSet);
                close(null, null, rs);
                close(null, null, rs2);
            }
            return tables;
        } catch (SQLException e) {
            throw new RuntimeException("mysql jdbc catch some error");
        } finally {
           close(connection, statement, null);
        }

    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        try {
            if (resultSet != null){
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (statement != null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (connection != null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
