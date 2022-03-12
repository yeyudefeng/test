package com.sqlparser.bean.inter.impl;

import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.FieldName;
import com.sqlparser.bean.bean.TableName;
import com.sqlparser.bean.inter.StatementParser;

import java.util.HashMap;
import java.util.List;

/**
 * 解析select类
 */
public abstract class StatementParserImpl implements StatementParser {
    public HashMap<String, DatabaseName> databaseNameMap = new HashMap<>();
    public HashMap<String, TableName> tableNameMap = new HashMap<>();
    public HashMap<String, FieldName> fieldNameMap = new HashMap<>();

    public void addDatabaseName(DatabaseName databaseName){
        if (databaseName == null){
            return;
        }
        DatabaseName dbn = databaseNameMap.getOrDefault(databaseName.getDatabaseName(), new DatabaseName(databaseName.getDatabaseName(), PermissionUtils.getEmptyPermissionSet()));
        dbn.getPermissionSet().addAll(databaseName.getPermissionSet());
        databaseNameMap.put(dbn.getDatabaseName(), dbn);
    }

    public void addTableName(TableName tableName){
        if (tableName == null){
            return;
        }
        TableName tbn = tableNameMap.getOrDefault(tableName.getTableName(), new TableName(tableName.getTableName(), PermissionUtils.getEmptyPermissionSet()));
        tbn.getPermissionSet().addAll(tableName.getPermissionSet());
        tableNameMap.put(tbn.getTableName(), tbn);
    }

    public void addDatabaseName(List<DatabaseName> databaseNameList){
        for (DatabaseName databaseName : databaseNameList){
            addDatabaseName(databaseName);
        }
    }
    public void addTableName(List<TableName> tableNameList){
        for (TableName tableName : tableNameList){
            addTableName(tableName);
        }
    }
}
