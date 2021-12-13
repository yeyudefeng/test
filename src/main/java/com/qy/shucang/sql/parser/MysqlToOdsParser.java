//package com.qy.shucang.sql.parser;
//
//import org.apache.commons.lang.StringUtils;
//
//import java.util.ArrayList;
//
//public class MysqlToOdsParser extends Parser{
//    private static String result = "select " + sep;
//    private static ArrayList<A> list = new ArrayList<A>();
//    private static Integer len = 30;
//    public void parse() {
//        String[] arr = sql.split(sep);
//        String tableNameLine = "";
//        Boolean isFlag = false;
//        for (int i= 0;i<arr.length;i++){
//            if (!StringUtils.isBlank(arr[i]) && !isFlag){
//                tableNameLine = arr[i];
//                isFlag = true;
//            }
//            String comment="";
//            String line = arr[i].trim();
//            if (!line.startsWith("CREATE TABLE")
//                    && line.startsWith("`")
//                    && !line.startsWith(") ENGINE=")
//                    && !line.startsWith("PRIMARY KEY")
//                    && !line.startsWith("UNIQUE KEY")
//                    && !line.startsWith("KEY")
//                    && !StringUtils.isBlank(line)
//            ){
//                String[] fields = line.split(" COMMENT ");
//                if (fields.length > 1 && !StringUtils.isBlank(fields[1])){
//                    comment = (fields[1].endsWith(",") ? fields[1].substring(0, fields[1].length() -1) : fields[1]).trim();
//                    comment = comment.substring(1, comment.length() - 1);
//                }
//                String field = fields[0].split(" {1,1000}")[0].replaceAll("`", "");
//                len = len > field.length() ? len : field.length();
//                list.add(new A(field, comment));
//            }
//        }
//        String tableName = tableNameLine.split("`")[1];
//        for (int j =0 ; j < list.size(); j++){
//            if (j != list.size()){
//                result += "    " + StringUtils.rightPad(list.get(j).f, len + 2, " ") + ", " + "-- " + list.get(j).c + sep;
//            } else {
//                result += "    " + StringUtils.rightPad(list.get(j).f, len + 2, " ") + "-- " + list.get(j).c + sep;
//            }
//
//        }
//        result += "from " + tableName;
//        System.out.println(result);
//    }
//
//    public static void main(String[] args) {
//        new MysqlToOdsParser().exec();
//    }
//}
