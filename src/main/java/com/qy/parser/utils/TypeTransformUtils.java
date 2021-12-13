package com.qy.parser.utils;

import org.apache.commons.lang.StringUtils;

public class TypeTransformUtils {
    public static String transfromType(String type) {
        if (StringUtils.isBlank(type)){
            throw new RuntimeException("type is null");
        }
        if (type.contains("varchar") || type.contains("char") || type.contains("text") || type.contains("string")){
            return "string";
        }
        if (type.contains("bigint")){
            return "bigint";
        }
        if (type.contains("tinyint")){
            return "tinyint";
        }
        if (type.contains("float") || type.contains("decimal") || type.contains("double")){
            return "double";
        }
        if (type.contains("int")){
            return "int";
        }
        if (type.contains("timestamp")){
            return "timestamp";
        }
        if (type.contains("date")){
            return "string";
        }
        if (type.contains("datetime") || type.contains("time")){
            return "datetime";
        }
        throw new RuntimeException("type " + type + " is unknown");
    }
}
