//package com.qy.shucang.sql.parser;
//
//import org.apache.commons.lang.StringUtils;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//
//public abstract class Parser {
//    private static final String fileName = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\sql1.txt";
//    private static final String databaseAndTableNameFile = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\databaseandtable.txt";
//    public static String sql = "";
//    public static String sep = "\n";
//    public static ArrayList<C> dtList = new ArrayList<C>();
//    public abstract void parse();
//
//    public void exec(){
//        readSql();
//        readTable();
//        parse();
//    }
//    private static void readSql(){
//        File file = new File(fileName);
//        BufferedReader reader = null;
//        try {
////            System.out.println("以行为单位读取文件内容，一次读一整行：");
//            reader = new BufferedReader(new FileReader(file));
//            String tempString = null;
//            int line = 1;
//            // 一次读入一行，直到读入null为文件结束
//            while ((tempString = reader.readLine()) != null) {
//                // 显示行号
////                System.out.println("line " + line + ": " + tempString);
//                sql += tempString + sep;
//                line++;
//            }
//            reader.close();
////            System.out.println("-------");
////            System.out.println(sql);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (reader != null) {
//                try {
//                    reader.close();
//                } catch (IOException e1) {
//                }
//            }
//        }
//    }
//
//    private static void readTable(){
//
//        File file = new File(databaseAndTableNameFile);
//        BufferedReader reader = null;
//        try {
//            reader = new BufferedReader(new FileReader(file));
//            String tempString = null;
//            int line = 1;
//            // 一次读入一行，直到读入null为文件结束
//            while ((tempString = reader.readLine()) != null) {
//                if (!StringUtils.isBlank(tempString)){
//                    dtList.add(new C(tempString.split("\\.")[0], tempString.split("\\.")[1]));
//                }
//                line++;
//            }
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (reader != null) {
//                try {
//                    reader.close();
//                } catch (IOException e1) {
//                }
//            }
//        }
//    }
//    private static void queryMysql(){
//
//    }
//
//    public static void main(String[] args) {
//        readSql();
//    }
//}
//class A{
//    public String f;
//    public String c;
//    public A(){}
//    public A(String f, String c) {
//        this.f = f;
//        this.c = c;
//    }
//}
//class B extends A{
//    public String t;
//
//    public B(String f, String t, String c) {
//        this.f = f;
//        this.t = t;
//        this.c = c;
//    }
//}
//
//class C {
//    public String d;
//    public String t;
//
//    public C(){}
//    public C(String d, String t) {
//        this.d = d;
//        this.t = t;
//    }
//}
//
//class D extends C{
//    public String sql;
//    public D(String d, String t, String sql) {
//        this.d = d;
//        this.t = t;
//        this.sql = sql;
//    }
//}