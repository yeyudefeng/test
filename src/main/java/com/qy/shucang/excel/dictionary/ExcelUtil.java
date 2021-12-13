package com.qy.shucang.excel.dictionary;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POI实现excel文件读写(导入/导出)操作工具类
 * @keywords java excel poi 导入 导出
 * @author zhuxiongxian
 * @version 1.0
 * @created at 2016年11月10日 下午2:10:57
 */
public class ExcelUtil {

    private static Logger mLogger = LoggerFactory.getLogger(ExcelUtil.class);


    /**
     * 根据workbook获取该workbook的所有sheet
     * @param wb
     * @return List<Sheet>
     */
    public static List<Sheet> getAllSheets(Workbook wb) {
        int numOfSheets = wb.getNumberOfSheets();
        List<Sheet> sheets = new ArrayList<Sheet>();
        for (int i = 0; i < numOfSheets; i++) {
            sheets.add(wb.getSheetAt(i));
        }
        return sheets;
    }

    /**
     * 根据excel文件来获取workbook
     * @param file
     * @return workbook
     * @throws IOException
     */
    public static Workbook getWorkbook(File file) throws IOException {
        Workbook wb = null;
        if (file.exists() && file.isFile()) {
            String fileName = file.getName();
            String extension = fileName.lastIndexOf(".") == -1 ? "" : fileName.substring(fileName.lastIndexOf(".") + 1); // 获取文件后缀

            if ("xls".equals(extension)) { // for office2003
                wb = new HSSFWorkbook(new FileInputStream(file));
            } else if ("xlsx".equals(extension)) { // for office2007
                wb = new XSSFWorkbook(new FileInputStream(file));
            } else {
                throw new IOException("不支持的文件类型");
            }
        }
        return wb;
    }

    /**
     * 根据excel文件来获取workbook
     * @param filePath
     * @return workbook
     * @throws IOException
     */
    public static Workbook getWorkbook(String filePath) throws IOException {
        File file = new File(filePath);
        return getWorkbook(file);
    }

    /**
     * 根据excel文件输出路径来获取对应的workbook
     * @param filePath
     * @return
     * @throws IOException
     */
    public static Workbook getExportWorkbook(String filePath) throws IOException {
        Workbook wb = null;
        File file = new File(filePath);

        String fileName = file.getName();
        String extension = fileName.lastIndexOf(".") == -1 ? "" : fileName.substring(fileName.lastIndexOf(".") + 1); // 获取文件后缀

        if ("xls".equals(extension)) { // for 少量数据
            wb = new HSSFWorkbook();
        } else if ("xlsx".equals(extension)) { // for 大量数据
            wb = new SXSSFWorkbook(5000); // 定义内存里一次只留5000行
        } else {
            throw new IOException("不支持的文件类型");
        }
        return wb;
    }

    /**
     * 根据workbook和sheet的下标索引值来获取sheet
     * @param wb
     * @param sheetIndex
     * @return sheet
     */
    public static Sheet getSheet(Workbook wb, int sheetIndex) {
        return wb.getSheetAt(sheetIndex);
    }

    /**
     * 根据workbook和sheet的名称来获取sheet
     * @param wb
     * @param sheetName
     * @return sheet
     */
    public static Sheet getSheet(Workbook wb, String sheetName) {
        return wb.getSheet(sheetName);
    }

    /**
     * 根据sheet返回该sheet的物理总行数
     * sheet.getPhysicalNumberOfRows方法能够正确返回物理的行数
     * @param sheet
     * @return
     */
    public static int getSheetPhysicalRowNum(Sheet sheet) {
        // 获取总行数
        int rowNum = sheet.getPhysicalNumberOfRows();
        return rowNum;
    }

    /**
     * 获取操作的行数
     * @param startRowIndex sheet的开始行位置索引
     * @param endRowIndex sheet的结束行位置索引
     * @return
     */
    public static int getSheetDataPhysicalRowNum(int startRowIndex, int endRowIndex) {
        int rowNum = -1;
        if (startRowIndex >= 0 && endRowIndex >= 0 && startRowIndex <= endRowIndex) {
            rowNum = endRowIndex - startRowIndex + 1;
        }
        return rowNum;
    }


    /**
     * 导出数据到Excel文件
     * @param dataList 要输出到Excel文件的数据集
     * @param filePath  excel文件输出路径
     */
    public static void exportExcel(String[][] dataList, String filePath) {
        try {
            // 声明一个工作薄
            Workbook workbook = getExportWorkbook(filePath);
            if (workbook != null) {
                // 生成一个表格
                Sheet sheet = workbook.createSheet();

                for (int i = 0; i < dataList.length; i++) {
                    String[] r = dataList[i];
                    Row row = sheet.createRow(i);
                    for (int j = 0; j < r.length; j++) {
                        Cell cell = row.createCell(j);
                        // cell max length 32767
                        if (r[j].length() > 32767) {
                            mLogger.warn("异常处理", "--此字段过长(超过32767),已被截断--" + r[j]);
                            r[j] = r[j].substring(0, 32766);
                        }
                        cell.setCellValue(r[j]);
                    }
                }
                // 自动列宽
                if (dataList.length > 0) {
                    int colcount = dataList[0].length;
                    for (int i = 0; i < colcount; i++) {
                        sheet.autoSizeColumn(i);
                    }
                }
                OutputStream out = new FileOutputStream(new File(filePath));
                workbook.write(out);
                out.close();
                workbook.close();
            }
        } catch (IOException e) {
            mLogger.error(e.toString(), e);
        }
    }

    /**
     * 利用JAVA的反射机制，将放置在JAVA集合中并且符号一定条件的数据以EXCEL 的形式输出到指定IO设备上<br>
     * 用于多个sheet
     * @param sheets ExcelSheet的集体
     * @param filePath excel文件路径
     */
    public static <T> void exportExcel(List<ExcelSheet<T>> sheets, String filePath) {
        exportExcel(sheets, filePath, null);
    }

    /**
     * 利用JAVA的反射机制，将放置在JAVA集合中并且符号一定条件的数据以EXCEL 的形式输出到指定IO设备上<br>
     * 用于多个sheet
     *
     * @param sheets    ExcelSheet的集合
     * @param filePath  excel文件输出路径
     * @param pattern   如果有时间数据，设定输出格式。默认为"yyy-MM-dd"
     */
    public static <T> void exportExcel(List<ExcelSheet<T>> sheets, String filePath, String pattern) {
        if (CollectionUtils.isEmpty(sheets)) {
            return;
        }
        try {
            // 声明一个工作薄
            Workbook workbook = getExportWorkbook(filePath);
            if (workbook != null) {
                for (ExcelSheet<T> sheetInfo : sheets) {
                    // 生成一个表格
                    Sheet sheet = workbook.createSheet(sheetInfo.getSheetName());
//                    sheet.setDefaultColumnWidth(5000 * 2560);
                    sheet.setColumnWidth(0, (100*15+323) * 2);
                    sheet.setColumnWidth(3, 200*15+323);
                    sheet.setColumnWidth(1, (252*15+323) * 2);
                    sheet.setColumnWidth(2, (252*15+323) * 2);
                    sheet.setColumnWidth(4, 150*15+323);
                    sheet.setColumnWidth(5, (222*15+323) * 2);
                    sheet.setColumnWidth(6, (222*15+323) * 2);
                    sheet.setColumnWidth(7, (222*15+323) * 2);
                    sheet.setColumnWidth(8, 200*15+323);
                    sheet.setDefaultRowHeight((short) (20 * 20));
                    write2Sheet(sheet, sheetInfo.getFirstLine(), sheetInfo.getHeaders(), sheetInfo.getDataset(), pattern);

                    for (RegionInfo regionInfo : sheetInfo.getRegionInfos()){
                        Integer lastRow = regionInfo.lastRow == Integer.MAX_VALUE ? sheet.getLastRowNum() : regionInfo.lastRow;
                        CellRangeAddress region = new CellRangeAddress(regionInfo.firstRow,lastRow,regionInfo.firstCol,regionInfo.lastCol);//合并从第rowFrom行columnFrom列
                        sheet.addMergedRegion(region);// 到rowTo行columnTo的区域
                    }
//                    if (!StringUtils.isBlank(((Info)sheetInfo.getDataset().iterator().next()).s)){
//                        CellRangeAddress region = new CellRangeAddress(2,sheet.getLastRowNum(),4,4);//合并从第rowFrom行columnFrom列
//                        sheet.addMergedRegion(region);// 到rowTo行columnTo的区域
//                    }
//                    CellRangeAddress region = new CellRangeAddress(2,sheet.getLastRowNum(),5,5);//合并从第rowFrom行columnFrom列
//                    sheet.addMergedRegion(region);// 到rowTo行columnTo的区域
//                    CellRangeAddress region1 = new CellRangeAddress(2,sheet.getLastRowNum(),6,6);//合并从第rowFrom行columnFrom列
//                    sheet.addMergedRegion(region1);// 到rowTo行columnTo的区域
//                    CellRangeAddress region2 = new CellRangeAddress(0,0,0,1);//合并从第rowFrom行columnFrom列
//                    sheet.addMergedRegion(region2);// 到rowTo行columnTo的区域
//                    CellRangeAddress region3 = new CellRangeAddress(0,0,2,3);//合并从第rowFrom行columnFrom列
//                    sheet.addMergedRegion(region3);// 到rowTo行columnTo的区域
//                    CellRangeAddress region4 = new CellRangeAddress(0,0,4,5);//合并从第rowFrom行columnFrom列
//                    sheet.addMergedRegion(region4);// 到rowTo行columnTo的区域
                }
                OutputStream out = new FileOutputStream(new File(filePath));
                workbook.write(out);

                out.close();
                workbook.close();
            }
        } catch (IOException e) {
            mLogger.error(e.toString(), e);
        }
    }

    /**
     * 每个sheet的写入
     * @param sheet   页签
     * @param headers 表头
     * @param dataset 数据集合
     * @param pattern 日期格式
     */
    public static <T> void write2Sheet(Sheet sheet, String[] firstLine, String[] headers, Collection<T> dataset, String pattern) {
        // 产生前缀行
        Row row = sheet.createRow(0);
        for (int i = 0; i < firstLine.length; i++) {
            Cell cell = row.createCell(i);
            cell.setCellValue(firstLine[i]);
        }
        // 产生表格标题行
        Row row1 = sheet.createRow(1);
        for (int i = 0; i < headers.length; i++) {
            Cell cell = row1.createCell(i);
            cell.setCellValue(headers[i]);
        }
        // 遍历集合数据，产生数据行
        Iterator<T> it = dataset.iterator();
        int index = 1;
        while (it.hasNext()) {
            index++;
            row = sheet.createRow(index);
            T t = (T) it.next();
            if (t instanceof Map) { // row data is map
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) t;
                int cellNum = 0;
                for (String k : headers) {
                    if (map.containsKey(k) == false) {
                        mLogger.error("Map 中 不存在 key [" + k + "]");
                        continue;
                    }
                    Cell cell = row.createCell(cellNum);
                    Object value = map.get(k);
                    if (value == null) {
                        cell.setCellValue(StringUtils.EMPTY);
                    } else {
                        cell.setCellValue(String.valueOf(value));
                    }
                    cellNum++;
                }
            } else if (t instanceof Object[]) { // row data is Object[]
                Object[] tObjArr = (Object[]) t;
                for (int i = 0; i < tObjArr.length; i++) {
                    Cell cell = row.createCell(i);
                    Object value = tObjArr[i];
                    if (value == null) {
                        cell.setCellValue(StringUtils.EMPTY);
                    } else {
                        cell.setCellValue(String.valueOf(value));
                    }
                }
            } else if (t instanceof List<?>) { // row data is List
                List<?> rowData = (List<?>) t;
                for (int i = 0; i < rowData.size(); i++) {
                    Cell cell = row.createCell(i);
                    Object value = rowData.get(i);
                    if (value == null) {
                        cell.setCellValue(StringUtils.EMPTY);
                    } else {
                        cell.setCellValue(String.valueOf(value));
                    }
                }
            } else { // row data is vo
                // 利用反射，根据javabean属性的先后顺序，动态调用getXxx()方法得到属性值
                Field[] fields = t.getClass().getDeclaredFields();
                for (int i = 0; i < fields.length; i++) {
                    Cell cell = row.createCell(i);
                    Field field = fields[i];
                    String fieldName = field.getName();
                    String getMethodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

                    try {
                        Class<?> tClazz = t.getClass();
                        Method getMethod = tClazz.getMethod(getMethodName, new Class[] {});
                        Object value = getMethod.invoke(t, new Object[] {});
                        String textValue = null;
                        if (value instanceof Integer) {
                            int intValue = (Integer) value;
                            cell.setCellValue(intValue);
                        } else if (value instanceof Float) {
                            float fValue = (Float) value;
                            cell.setCellValue(fValue);
                        } else if (value instanceof Double) {
                            double dValue = (Double) value;
                            cell.setCellValue(dValue);
                        } else if (value instanceof Long) {
                            long longValue = (Long) value;
                            cell.setCellValue(longValue);
                        } else if (value instanceof Boolean) {
                            boolean bValue = (Boolean) value;
                            cell.setCellValue(bValue);
                        } else if (value instanceof Date) {
                            Date date = (Date) value;
                            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                            textValue = sdf.format(date);
                        } else {
                            // 其它数据类型都当作字符串简单处理
                            textValue = value.toString();
                        }
                        if (textValue != null) {
                            // HSSFRichTextString richString = new
                            // HSSFRichTextString(textValue);
                            cell.setCellValue(textValue);
                        } else {
                            cell.setCellValue(StringUtils.EMPTY);
                        }
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (SecurityException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }
            }
        }
        // 设定自动宽度
        for (int i = 0; i < headers.length; i++) {
//            sheet.autoSizeColumn(1, true);
//            sheet.autoSizeColumn(i);
        }
    }



    /**
     * 分别测试以下示例并成功通过：
     * 1. 单个sheet， 数据集类型是List<List<Object>>
     * 2. 单个sheet， 数据集类型是List<Object[]>
     * 3. 多个sheet， 数据集类型是List<ExcelSheet<List<Object>>>
     * 4. 多个sheet， 数据集类型是List<ExcelSheet<List<Object>>>
     * 5. 多个sheet， 数据集类型是List<ExcelSheet<List<Object>>>, 支持大数据量
     * @param args
     */
    public static void main(String[] args) {
        // List<List<Object>> list = new ArrayList<List<Object>>();

        /*try {

            // list = readExcel(new File("D:/test.xlsx"));
            // 导入
            // list = readExcel(new File("D:/test.xlsx"), 1);
            list = readExcelBody("D:/test.xlsx", 1);

            List<Object[]> dataList = new ArrayList<Object[]>();
            for (int i = 0; i < list.size(); i++) {
                Object[] objArr = new Object[list.get(i).size()];
                List<Object> objList = list.get(i);
                for (int j = 0; j < objList.size(); j++) {
                    objArr[j] = objList.get(j);
                }
                dataList.add(objArr);
            }
            for (int i = 0; i < dataList.size(); i++) {
            	System.out.println(Arrays.toString(dataList.get(i)));
            }

            String[] headers = { "代理商ID", "代理商编码", "系统内代理商名称", "贷款代理商名称", "入网时长", "佣金账期", "佣金类型", "金额" };
            String filePath = "d://out_" + System.currentTimeMillis() + ".xlsx";

            ExcelSheet<List<Object>> sheet = new ExcelSheet<List<Object>>();
            sheet.setHeaders(headers);
            sheet.setSheetName("按入网时间提取佣金数");
            sheet.setDataset(list);

            ExcelSheet<Object[]> sheet = new ExcelSheet<Object[]>();
            sheet.setHeaders(headers);
            sheet.setSheetName("按入网时间提取佣金数");
            sheet.setDataset(dataList);

            // List<ExcelSheet<List<Object>>> sheets = new
            // ArrayList<ExcelSheet<List<Object>>>();
            List<ExcelSheet<Object[]>> sheets = new ArrayList<ExcelSheet<Object[]>>();
            sheets.add(sheet);
            // 导出
            // exportExcel(headers, list, os);
            // exportExcel(headers, dataList, os);
            exportExcel(sheets, filePath);
            // list = readExcel(new File("D:/test.xlsx"), "按入网时间提取佣金数");
            // list = readExcel(new File("D:/test.xlsx"), 0, 1, 85, 0, 6);
            // list = readExcel(new File("D:/test.xlsx"), "按入网时间提取佣金数", 1061,
            // 1062, 0, 8);
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        // 有3个sheet的数据， 每个sheet数据为50万行， 共150万行数据输出到Excel文件, 性能测试。
        List<ExcelSheet<List<Object>>> sheetsData = new ArrayList<ExcelSheet<List<Object>>>();

        int sheetRowNum = 50;

        for (int i = 0; i < 3; i++) {
            ExcelSheet<List<Object>> sheetData = new ExcelSheet<List<Object>>();
            String[] headers = { "姓名", "手机号码", "性别", "身份证号码", "家庭住址" };
            String sheetName = "第" + (i + 1) + "个sheet";

            List<List<Object>> sheetDataList = new ArrayList<List<Object>>();
            for (int j = 0; j < sheetRowNum; j++) {
                List<Object> rowData = new ArrayList<Object>();
                rowData.add("小明");
                rowData.add("18888888888");
                rowData.add("男");
                rowData.add("123123123123123123");
                rowData.add("广州市");
                sheetDataList.add(rowData);
            }
            sheetData.setSheetName(sheetName);
            sheetData.setHeaders(headers);
            sheetData.setDataset(sheetDataList);

            sheetsData.add(sheetData);
        }
        String filePath = "d://out_" + System.currentTimeMillis() + ".xlsx";
        exportExcel(sheetsData, filePath);
        System.out.println("-----end-----");
        /*for (int i = 0; i < list.size(); i++) {
        	System.out.println(list.get(i));
        }*/
    }

}
