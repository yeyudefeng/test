package com.qy.shucang.excel.dictionary;
/**
 * 解析svn的dim create sql，同时寻找对应的insert sql。写入excel。
 */
public class ExcelDimCreateParser extends ExcelDwCreateParser {
    public ExcelDimCreateParser (DicEnum dicEnum){
        this.dicEnum = dicEnum;
    }
}
