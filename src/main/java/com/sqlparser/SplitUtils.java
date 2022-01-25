package com.sqlparser;

import com.sqlparser.bean.Constant;
import com.sqlparser.bean.SqlParserRuntimeException;
import org.apache.commons.lang.StringUtils;

public class SplitUtils {
    private static final String POINT = ".";
    private static final String SPLIT_POINT = "\\.";
    // db.tb || tb || tb.f || f
    public static String getLeft(String line){
        checkArrayLength(line);
        String[] arr = line.split(SPLIT_POINT);
        return arr.length == 2 ? arr[0] : "";
    }
    public static String getRight(String line){
        checkArrayLength(line);
        String[] arr = line.split(SPLIT_POINT);
        return arr.length == 2 ? arr[1] : arr[0];
    }
    private static String checkArrayLength(String line){
        if (StringUtils.isBlank(line)){
            return null;
        }
        String[] arr = line.split(SPLIT_POINT);
        if (arr.length > 2){
            throw new SqlParserRuntimeException(Constant.ERROR_ARRAY_TOO_LONG);
        }
        return line;
    }
}
