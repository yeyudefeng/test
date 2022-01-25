package com.sqlparser.bean;

import com.qy.parser.utils.FileUtils;

public class TestSql {
    public static String sql = FileUtils.read("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\testsql.txt");
    public static String sql2 = FileUtils.read("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\testsql2.txt");

    public static void main(String[] args) {
        System.out.println(sql);
    }
}
