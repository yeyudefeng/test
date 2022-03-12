package com.sqlparser.bean.bean;

import java.util.HashSet;

public class TableName extends DataName{
    public String tableName;

    public TableName(String tableName, HashSet<Permission> permissionSet) {
        super(permissionSet);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
