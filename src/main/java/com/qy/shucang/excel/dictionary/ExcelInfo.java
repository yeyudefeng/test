package com.qy.shucang.excel.dictionary;

public class ExcelInfo {
    public String i;// index
    public String c;//comment
    public String f;//field
    public String t;//type
    public String p;//is primary key
    public String s;//source
    public String sql;//insert / select sql
    public String csql;//create sql
    public String o;//other

//    public ExcelInfo(String i, String f, String t, String c, String s, String sql, String csql, String o) {
//        this.i = i;
//        this.f = f;
//        this.t = t;
//        this.c = c;
//        this.s = s;
//        this.sql = sql;
//        this.csql = csql;
//        this.o = o;
//    }

    public ExcelInfo(String i, String c, String f, String t, String p, String s, String sql, String csql, String o) {
        this.i = i;
        this.c = c;
        this.f = f;
        this.t = t;
        this.p = p;
        this.s = s;
        this.sql = sql;
        this.csql = csql;
        this.o = o;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getI() {
        return i;
    }

    public void setI(String i) {
        this.i = i;
    }

    public String getF() {
        return f;
    }

    public void setF(String f) {
        this.f = f;
    }

    public String getT() {
        return t;
    }

    public void setT(String t) {
        this.t = t;
    }

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getCsql() {
        return csql;
    }

    public void setCsql(String csql) {
        this.csql = csql;
    }

    public String getO() {
        return o;
    }

    public void setO(String o) {
        this.o = o;
    }
}
