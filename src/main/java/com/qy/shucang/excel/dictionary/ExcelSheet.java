package com.qy.shucang.excel.dictionary;

import java.util.Collection;
import java.util.List;

/**
 * 用于汇出多个sheet的Vo
 *
 * @author zhuxiongxian
 * @version 1.0
 * @created at 2016年12月14日 下午4:25:53
 */
public class ExcelSheet<T> {

    private String sheetName; // sheet的名字
    private String[] headers; // sheet的表头
    private String[] firstLine; // sheet的第一行
    private Collection<T> dataset; // sheet的数据集
    private List<RegionInfo> regionInfos; // sheet的数据集
    private Class className; // sheet的数据集

    public Class getClassName() {
        return className;
    }

    public void setClassName(Class className) {
        this.className = className;
    }

    public List<RegionInfo> getRegionInfos() {
        return regionInfos;
    }

    public void setRegionInfos(List<RegionInfo> regionInfos) {
        this.regionInfos = regionInfos;
    }

    public String[] getFirstLine() {
        return firstLine;
    }

    public void setFirstLine(String[] firstLine) {
        this.firstLine = firstLine;
    }

    /**
     * @return the sheetName
     */
    public String getSheetName() {
        return sheetName;
    }

    /**
     * @param sheetName
     *            the sheetName to set
     */
    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    /**
     * @return the headers
     */
    public String[] getHeaders() {
        return headers;
    }

    /**
     * @param headers
     *            the headers to set
     */
    public void setHeaders(String[] headers) {
        this.headers = headers;
    }

    /**
     * @return the dataset
     */
    public Collection<T> getDataset() {
        return dataset;
    }

    /**
     * @param dataset
     *            the dataset to set
     */
    public void setDataset(Collection<T> dataset) {
        this.dataset = dataset;
    }

}
