package com.qy.shucang.excel.createsql;

import com.qy.shucang.excel.format.Field;

import java.util.ArrayList;

public class DT{
    public String database;
    public String tablename;
    public String sql;
    public ArrayList<Field> fields;
    public String tableComment;

    public DT(String database, String tablename) {
        this.database = database;
        this.tablename = tablename;
    }

    public DT(String database, String tablename, String sql, ArrayList<Field> fields, String tableComment) {
        this.database = database;
        this.tablename = tablename;
        this.sql = sql;
        this.fields = fields;
        this.tableComment = tableComment;
    }
}