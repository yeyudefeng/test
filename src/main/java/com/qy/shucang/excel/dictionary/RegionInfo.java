package com.qy.shucang.excel.dictionary;

public class RegionInfo {
    public int firstRow;
    public int lastRow;
    public int firstCol;
    public int lastCol;

    public RegionInfo(int firstRow, int lastRow, int firstCol, int lastCol) {
        this.firstRow = firstRow;
        this.lastRow = lastRow;
        this.firstCol = firstCol;
        this.lastCol = lastCol;
    }
}
