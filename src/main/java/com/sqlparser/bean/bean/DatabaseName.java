package com.sqlparser.bean.bean;

import java.util.HashSet;

public class DatabaseName extends DataName{
    private String databaseName;

    public DatabaseName(String databaseName, HashSet<Permission> permissionSet) {
        super(permissionSet);
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
