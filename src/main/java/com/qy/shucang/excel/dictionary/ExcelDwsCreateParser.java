package com.qy.shucang.excel.dictionary;
/**
 * 解析svn的dws create sql，同时寻找对应的insert sql。写入excel。
 */
public class ExcelDwsCreateParser extends ExcelDwCreateParser {
    public ExcelDwsCreateParser (DicEnum dicEnum){
        this.dicEnum = dicEnum;
    }
}
