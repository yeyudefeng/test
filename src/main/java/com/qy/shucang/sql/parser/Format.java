//package com.qy.shucang.sql.parser;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//
//public abstract class Format {
//    private static final String fileName = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\insertsql.txt";
//
//    public static String sql = "";
//    public static String sep = "\n";
//    public abstract void format();
//    public void exec(){
//        readSql();
//        format();
//    }
//
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
//}
