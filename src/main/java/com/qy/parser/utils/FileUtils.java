package com.qy.parser.utils;

import com.qy.parser.enums.EInfo;

import java.io.*;

/**
 * 读取文件和写入文件工具类
 */
public class FileUtils {

    /**
     * 读取文件
     * @param filePath
     * @return
     */
    public static String read(String filePath){
        StringBuffer context = new StringBuffer();
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                context.append(tempString).append(EInfo.FILE_SEP);
                line++;
            }
            reader.close();
            return context.toString();
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

    /**
     * 写入文件
     * @param fileName
     * @param value
     */
    public static Boolean write(String fileName, String value){
        try {
            FileOutputStream fos = new FileOutputStream(fileName);
            String s = value;
            fos.write(s.getBytes());
            fos.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return true;
    }
}
