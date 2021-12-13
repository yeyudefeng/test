package com.qy.shucang.excel.dictionary;

import java.io.*;

public class JSONParser {
    public String context = "";
    public String sep = "\n";
    public void parse(){}
    public void exec(){
        parse();
    }
    public void read(String filePath){
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
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    public void write(String fileName, String value){
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
