package com.qy.shucang.excel.dictionary;

import java.io.*;

/**
 * 读取文件和写入文件工具类
 */
public class FileUtils {
    public static String sep = "\n";
    public static String read(String filePath){
        String context = "";
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                context += tempString + sep;
                line++;
            }
            reader.close();
            return context;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("read file catch some error");
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    public static void write(String fileName, String value){
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            String s = value;
            fos.write(s.getBytes());
            fos.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
