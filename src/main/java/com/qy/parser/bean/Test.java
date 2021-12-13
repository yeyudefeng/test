package com.qy.parser.bean;

import com.alibaba.druid.sql.dialect.hive.parser.HiveSelectParser;
import com.qy.parser.utils.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.qy.parser.enums.EInfo.*;

public class Test {

    private static Element body;

    public static void main(String[] args) throws IOException {
        Document doc = Jsoup.parse(readHtml("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\html\\test.html"));
        body = doc.body();
        body.select("");

        System.out.println("=======");

    }

    public static String readHtml(String fileName){
        FileInputStream fis = null;
        StringBuffer sb = new StringBuffer();
        try {
            fis = new FileInputStream(fileName);
            byte[] bytes = new byte[1024];
            while (-1 != fis.read(bytes)) {
                sb.append(new String(bytes));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return sb.toString();
    }


}
