package com.qy.shucang.excel.dictionary;
/**
 * 解析svn的dwd create sql，同时寻找对应的insert sql。写入excel。
 */
public class ExcelDwdCreateParser extends ExcelDwCreateParser {
    public ExcelDwdCreateParser (DicEnum dicEnum){
        this.dicEnum = dicEnum;
    }
}
