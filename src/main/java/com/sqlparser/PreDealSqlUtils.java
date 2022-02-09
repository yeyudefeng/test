package com.sqlparser;

/**
 * 对sql进行预处理。以使druid可以识别解析
 */
public class PreDealSqlUtils {
    public static String dealSql(String sql){
        String result = "";
        result = dealOuter(sql);
        result = dealMiddleBrackets(result);
        return result;
    }

    /**
     * 处理outer，druid不识别outer，解析时会报错
     *       )t lateral view outer explode(t.tags_normal) h as tags_normal
     * @param sql
     * @return
     */
    private static String dealOuter(String sql){
        return sql.replaceAll("lateral( |\t|\n){1,100}view( |\t|\n){1,100}outer( |\t|\n){1,100}explode", "lateral view explode");
    }

    /**
     * 处理druid识别不了中括号的问题，解析时会报错
     * 	collect_set ( grade_name ) [ 0 ] AS grade_name,
     * 	split('abcdef', 'c')[]
     * @param sql
     * @return
     */
    private static String dealMiddleBrackets(String sql){
        return sql.replaceAll("\\[( |\t|\n){1,100}[0-9]{1,5}( |\t|\n){1,100}\\]"," ");
    }
}
