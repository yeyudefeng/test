package com.qy.parser.utils;

import com.qy.parser.enums.EInfo;
import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    public static Properties properties = null;
    private static void init() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(EInfo.BASE_PROPERTIES_FILE_PATH));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public static String getValue(String key){
        if (properties == null){
            init();
        }
        return properties.getProperty(key);
    }
    public static String getValueNotNull(String key){
        if (properties == null){
            init();
        }
        String property = properties.getProperty(key);
        if (!StringUtils.isBlank(property)){
            return property;
        }
        throw new RuntimeException(key + " is null, please give this value in common.properties");
    }

    public static void main(String[] args) {
        init();
    }
}
