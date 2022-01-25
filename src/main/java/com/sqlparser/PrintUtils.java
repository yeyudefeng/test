package com.sqlparser;

import com.sqlparser.bean.Constant;
import com.sqlparser.bean.SqlParserRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintUtils {
    private static final Logger logger = LoggerFactory.getLogger(PrintUtils.class);

    public static void printMethodName(Object object) {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();//调用该方法的方法名
        int lineNumber = Thread.currentThread().getStackTrace()[2].getLineNumber();//调用该方法的方法的代码所在的行数
        System.out.println("当前问题定位为：( " + methodName + " ) 方法，类型不匹配" +
                "方法调用的代码行数为第 ( " + lineNumber + " ) 行" +
                "\n请Debug该SQL，并在该方法里面增加对象的类型判断，实现相应的逻辑即可。");
        logger.error("当前问题定位为：( " + methodName + " ) 方法，类型不匹配" +
                "方法调用的代码行数为第 ( " + lineNumber + " ) 行" +
                "\n请Debug该SQL，并在该方法里面增加对象的类型判断，实现相应的逻辑即可。");
        if (object != null) {
            String objClass = object.getClass().toString();
            System.out.println("当前对象的类型为：( " + objClass + " )" +
                    "\n请强转为该类型，实现类似于其他类型的逻辑代码。");
            logger.error("当前对象的类型为：( " + objClass + " )" +
                    "\n请强转为该类型，实现类似于其他类型的逻辑代码。");
        } else {
            System.out.println("当前对象 ( " + object + " ) 为null");
            logger.error("当前对象 ( " + object + " ) 为null");
        }
        throw new SqlParserRuntimeException(Constant.VALUE_IS_NULL + " : " + object);
    }
}
