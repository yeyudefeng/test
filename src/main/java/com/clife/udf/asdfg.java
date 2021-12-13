//package com.clife.udf;
//
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.InputStreamReader;
//import java.io.*;
//import java.util.ArrayList;
//import java.util.List;
//
//import jxl.Workbook;
//import jxl.write.Label;
//import jxl.write.WritableSheet;
//import jxl.write.WritableWorkbook;
//import jxl.write.WriteException;
//
//
//public class getTabel2Excel {
//    public static void main(String[] args) throws IOException {
//        getTabel2Excel getTabel2Excel = new getTabel2Excel();
//        //输入路径
//        String inputPath = "D:\\项目\\华润数仓资料\\dim文件\\建表";
//        //输出路径
//        String outputPath = "D:\\项目\\华润数仓资料\\dim文件\\建表\\excexl88151.xls";
//        //获取其file对象
//        File file = new File(inputPath);
//        File filere = new File(outputPath);
//        //递归遍历，返回所有路径
//        String urlStr = func(file);
//        //输出文件
//        FileOutputStream os = new FileOutputStream(filere);
//        //创建工作薄
//        WritableWorkbook workbook = Workbook.createWorkbook(os);
//        //切割路径，传入
//        String[] urlSpl = urlStr.split("&");
//        for (int i = 0; i < urlSpl.length; i++) {
//            //获取表字段内容
//            String jsonBeen = getTabel2Excel.getTxt2Been(urlSpl[i]);
//            //生成excel表
//            getTabel2Excel.getBeen2Ecel(jsonBeen, workbook, i, urlSpl.length - 1);
//        }
//        //IO关闭
//        os.close();
//    }
//
//    /**
//     * 遍历读取文件夹下的文件
//     *
//     * @param file
//     * @return
//     */
//    public static String func(File file) {
//        StringBuffer urlStrBuff = new StringBuffer();
//        File[] fs = file.listFiles();
//        for (File f : fs) {
//            if (f.isDirectory())    //若是目录，则递归打印该目录下的文件
//                func(f);
//            if (f.isFile()) {     //若是文件，直接打印
//                //System.out.println(f);
//                urlStrBuff.append(f + "&");
//            }
//        }
//        System.out.println(urlStrBuff.toString());
//        return urlStrBuff.toString();
//    }
//
//    /**
//     * 创建excel表
//     *
//     * @param jsonFlie
//     */
//    public void getBeen2Ecel(String jsonFlie, WritableWorkbook workbook, int num, int maxNum) {
//        String[] jsonStr = jsonFlie.split("&");
//        List<String> data = new ArrayList<>();
//        List<String> title = new ArrayList<>();
//        title.add("序号");
//        title.add("字段名称");
//        title.add("字段英文");
//        title.add("字段类型");
//        title.add("来源表");
//        title.add("来源逻辑");
//        title.add("备注");
//        title.add("主键");
//        for (int i = 0; i < jsonStr.length; i++) {
//            if (!jsonStr[i].split(",")[2].equals("null")) {
//                data.add(jsonStr[i]);
//            }
//        }
//        putExcel(title, data, workbook, num, maxNum);
//    }
//
//    public static void putExcel(List<String> title, List<String> data, WritableWorkbook workbook, int num, int maxNum) {
//        try {
//            createExcel(title, data, workbook, num, maxNum);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void createExcel(List<String> title, List<String> data, WritableWorkbook workbook, int num, int maxNum) throws WriteException, IOException {
//        String[] connectStr1 = data.get(1).split(",");
//        //创建新的一页
//        WritableSheet sheet = workbook.createSheet(connectStr1[5], num);
//        //第一行，表名，创建文本表头
//        Label sourceTbaleEn = new Label(0, 0, connectStr1[4]);
//        Label sourceTbaleCN = new Label(1, 0, connectStr1[5]);
//        sheet.addCell(sourceTbaleEn);
//        sheet.addCell(sourceTbaleCN);
//        // 创建文本
//        for (int i = 0; i < title.size(); i++) {
//            Label tou = new Label(i, 1, title.get(i));
//            sheet.addCell(tou);
//        }
//        for (int i = 0; i < data.size(); i++) {
//            String[] connectStr = data.get(i).split(",");
//            for (int j = 0; j < 4; j++) {
//                Label connect = new Label(j, i + 2, connectStr[j]);
//                sheet.addCell(connect);
//            }
//        }
//        //遍历完，再写入，关闭
//        if (num == maxNum) {
//            //把创建的内容写入到输出流中，并关闭输出流
//            workbook.write();
//            workbook.close();
//        }
//    }
//
//    /**
//     * 获取文件内容
//     */
//    public String getTxt2Been(String file) throws IOException {
//        StringBuffer jsonStr = new StringBuffer();
//        String schemaName = null;
//        String tableName = null;
//        String tableComment = null;
//        textBean textBeen = new textBean();
//        InputStreamReader ins = new InputStreamReader(new FileInputStream(file));
//        BufferedReader br = new BufferedReader(ins);
//        //读取txt
//        String line = null;
//        List<String> list = new ArrayList<String>();
//        while ((line = br.readLine()) != null) {
//            list.add(line);
//        }
//        for (int i = 0; i < list.size(); i++) {
//            System.out.println(list.get(i));
//        }
//        br.close();
//        //将每个字段，存到StringBuffer
//        for (int i = 0; i < list.size(); i++) {
//            if (list.get(i).contains("create")) {
//                //txt的每一行相当于一条数据，split按空格作分隔符进行拆分。\\s+是正则表达式。
//                String[] arrStr1 = list.get(i).split("\\s+");
//                for (int k = 0; k < arrStr1.length; k++) {
//                    if (arrStr1[k].contains(".")) {
//                        String[] tables = arrStr1[k].split("\\.");
//                        schemaName = tables[0];
//                        tableName = tables[1].replace("(", "");
//                    }
//                }
//                continue;
//            }
//            if (list.get(i).contains(")comment")) {
//                String[] arrStr1 = list.get(i).split("\\s+");
//                tableComment = arrStr1[1];
//            }
//        }
//        int flag = 0;
//        for (int i = 0; i < list.size(); i++) {
//            textBeen.setSchemaName(schemaName);
//            textBeen.setTableName(tableName);
//            textBeen.setTableComment(tableComment);
//            if (list.get(i).contains("comment")) {
//                flag = flag + 1;
//                String[] arrStr = list.get(i).split("\\s+");
//                if (arrStr[0].equals(")comment")) {
//                    break;
//                }
//                //因为是从create开始的，序号要从comment开始，break结束，不能再写入到been对象中
//                textBeen.setColumnId(String.valueOf(flag));
//                if (arrStr[0].equals("")) {
//                    textBeen.setColumnName(arrStr[1]);
//                    textBeen.setColumnDataType(arrStr[2]);
//                    textBeen.setComment(arrStr[4]);
//                } else {
//                    textBeen.setColumnName(arrStr[0].replace(",", ""));
//                    textBeen.setColumnDataType(arrStr[1]);
//                    textBeen.setComment(arrStr[3]);
//                }
//            }
//            //追加到StringBuffer中
//            jsonStr.append(textBeen + "&");
//        }
//        System.out.println(jsonStr.deleteCharAt(jsonStr.length() - 1).toString().replaceAll("'", ""));
//        //替换掉单引号
//        return jsonStr.toString().replaceAll("'", "");
//    }
//}
//
//
//
