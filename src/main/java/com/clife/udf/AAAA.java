package com.clife.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import jxl.Workbook;
import jxl.format.UnderlineStyle;
import jxl.write.Label;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;


public class AAAA
{
    //读取的txt文件路径
    private static String txtFilePath = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\aaa.txt";

    //生成的excel文件路径
    private static String excelFilePath = new StringBuffer().append("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\")
            .append("数据").append(new SimpleDateFormat("YYYYMMdd").format(new Date())).append(".xls").toString();

    //编码格式
    private static String encoding = "GBK";



    public static void readAndWrite(String filePath)
    {
        try
        {
            File file = new File(filePath);


            File tempFile = new File(excelFilePath);

            //判断文件是否存在
            if (file.isFile() && file.exists())
            {
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);//考虑到编码格式


                BufferedReader bufferedReader = new BufferedReader(read);

                WritableWorkbook workbook = Workbook.createWorkbook(tempFile);

                WritableSheet sheet = workbook.createSheet("Sheet1", 0);

                //一些临时变量，用于写到excel中
                Label l = null;

                String lineTxt = null;

                //设置字体为宋体，11号
                WritableFont headerFont = new WritableFont(WritableFont.createFont("宋体"), 11, WritableFont.NO_BOLD, false, UnderlineStyle.NO_UNDERLINE, jxl.format.Colour.BLACK);

                WritableCellFormat headerFormat = new WritableCellFormat (headerFont);

                int column = 0;
                int i = 0;

                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    l = new Label(column++, i, lineTxt, headerFormat);
                    sheet.addCell(l);

                    //判断内容是否为空行，如果是，则转行
                    if("".equals(lineTxt))
                    {
                        i++;
                        column = 0;
                        continue;
                    }
                }
                //设置单元格宽度
                sheet.setColumnView(0, 20);
                sheet.setColumnView(1, 35);
                sheet.setColumnView(2, 15);

                //写入文件
                workbook.write();
                //关闭文件
                workbook.close();
                read.close();
            }
            else
            {
                System.out.println("找不到指定的文件");
            }
        }
        catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }


    }


    public static void main(String argv[])
    {
//        readAndWrite(txtFilePath);
        String a = "aa tinyint(1) comment";
        String b = a.replaceAll(" tinyint\\([0-9]{1,10}\\) ", " tinyint ");
        System.out.println(b);
    }
}
