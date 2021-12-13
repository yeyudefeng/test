package com.clife.udf;

import java.sql.*;

public class JDBC {

     public static void main(String[] args) throws ClassNotFoundException, SQLException {
          String sql = "select * from hbasetohdfs.bbb limit 10";
//          String sql = args[0];
          Class.forName("org.apache.hive.jdbc.HiveDriver");
          Connection connection = DriverManager.getConnection("jdbc:hive2://200.200.200.65:10000/default", "hive", "hive");
          Statement statement = connection.createStatement();

          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()){
               System.out.println(resultSet.getString(1));
               return;
          }

          statement.execute(args[0]);

          resultSet.close();
          statement.close();
          connection.close();
     }
}
