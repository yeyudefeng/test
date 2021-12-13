package com.qy.parser.bean;


import java.util.ArrayList;

public class EDT {
    public String database;
    public String tablename;
    public String sql;
    public ArrayList<EField> fields;
    public ArrayList<EField> partitions;
    public String tableComment;

    public EDT(String database, String tablename, String tableComment) {
        this.database = database;
        this.tablename = tablename;
        this.tableComment = tableComment;
    }

    public EDT() {
    }

    public EDT(String database, String tablename) {
        this.database = database;
        this.tablename = tablename;
    }

    public EDT(String database, String tablename, String sql, ArrayList<EField> fields, String tableComment) {
        this.database = database;
        this.tablename = tablename;
        this.sql = sql;
        this.fields = fields;
        this.tableComment = tableComment;
    }
}