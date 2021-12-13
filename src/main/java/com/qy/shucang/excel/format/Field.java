package com.qy.shucang.excel.format;

public class Field{
    public String l;//field
    public String m;//type
    public String r;//comment

    public Field(String l, String r) {
        this.l = l;
        this.r = r;
    }

    public Field(String l, String m, String r) {
        this.l = l;
        this.m = m;
        this.r = r;
    }
}