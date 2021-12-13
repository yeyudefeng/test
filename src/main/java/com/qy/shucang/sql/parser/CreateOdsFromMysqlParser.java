//package com.qy.shucang.sql.parser;
//
//import org.apache.commons.lang.StringUtils;
//
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//public class CreateOdsFromMysqlParser extends Parser{
//    private static String createTableStr = "CREATE TABLE";
//    private static HashMap<String, D> map = new HashMap<String, D>();
//    public void parse() {
//        String[] arr = sql.split(createTableStr);
//        for (String createSql : arr){
//            if (StringUtils.isBlank(createSql)){
//                continue;
//            }
//            createSql = createTableStr + createSql;
//            D d = parseCreateSql(createSql);
//            map.put(d.t, d);
//        }
//        for (C c : dtList){
//
//            String tableName = c.t;
//            tableName = tableName.startsWith("tb_") ? "clife_campus_ods_" + tableName.substring(3) : tableName;
//            String sql = map.get(c.t).sql.replace(c.t, tableName);
//            System.out.println("--- " + c.d + "." + tableName + "  --- " + c.t);
//            System.out.println(sql);
//            System.out.println("\n");
//
//        }
//
//    }
//
//    private D parseCreateSql(String createSql) {
//        String result = "create table if not exists ";
//        Integer len = 30;
//        ArrayList<B> list = new ArrayList<B>();
//        String[] arr = createSql.split(sep);
//        String tableNameLine = "";
//        String tableCommentLine = "";
//        Boolean isFlag = false;
//        for (String line : arr){
//            if (!StringUtils.isBlank(line) && !isFlag){
//                tableNameLine = line;
//                isFlag = true;
//            }
//            if (!StringUtils.isBlank(line) && line.trim().startsWith(") ENGINE=")){
//                tableCommentLine = line;
//            }
//            line = line.trim();
//            if (!line.startsWith("CREATE TABLE")
//                    && line.startsWith("`")
//                    && !line.startsWith(") ENGINE=")
//                    && !line.startsWith("PRIMARY KEY")
//                    && !line.startsWith("UNIQUE KEY")
//                    && !line.startsWith("KEY")
//                    && !StringUtils.isBlank(line)
//            ){
//                String comment="";
//                String[] fields = line.split(" COMMENT ");
//                if (fields.length > 1 && !StringUtils.isBlank(fields[1])){
//                    comment = (fields[1].endsWith(",") ? fields[1].substring(0, fields[1].length() -1) : fields[1]).trim();
//                    comment = comment.substring(1, comment.length() - 1);
//                }
//                String field = fields[0].split(" {1,1000}")[0].replaceAll("`", "");
//                String type = fields[0].split(" {1,1000}")[1].replaceAll("`", "");
//                type = transfromType(type);
//                len = len > field.length() ? len : field.length();
//                list.add(new B(field, type, comment));
//            }
//        }
//        String tableName = tableNameLine.split("`")[1];
//        result += tableName + " ( " + sep;
//        for (int j =0 ; j < list.size(); j++){
//            result += "    " + StringUtils.rightPad("`" + list.get(j).f + "`", len + 2, " ")
//                    + StringUtils.rightPad(list.get(j).t, len + 2, " ") + "comment \"" + list.get(j).c + "\"" + sep;
//        }
//        String temp = tableCommentLine.split("COMMENT=")[1];
//        String tableComment = temp.substring(1, temp.length() - 2);
//        result = result + ") comment '" + tableComment + "' " + sep;
//        result += "partitioned by ( part_date string ) " + sep +
//                "row format delimited fields terminated by '\\t' " + sep +
//                "stored as parquet " + sep + ";";
//        return new D(null, tableName, result);
//    }
//
//    private String transfromType(String type) {
//        if (StringUtils.isBlank(type)){
//            throw new RuntimeException("type is null");
//        }
//        if (type.contains("varchar") || type.contains("char") ){
//            return "string";
//        }
//        if (type.contains("bigint")){
//            return "bigint";
//        }
//        if (type.contains("tinyint")){
//            return "tinyint";
//        }
//        if (type.contains("float") || type.contains("decimal") || type.contains("double")){
//            return "double";
//        }
//        if (type.contains("int")){
//            return "int";
//        }
//        if (type.contains("timestamp")){
//            return "timestamp";
//        }
//        if (type.contains("date")){
//            return "date";
//        }
//        if (type.contains("datetime") || type.contains("time")){
//            return "datetime";
//        }
//        throw new RuntimeException("type " + type + " is unknown");
//    }
//
//    public static void main(String[] args) {
//        new CreateOdsFromMysqlParser().exec();
//    }
//}
