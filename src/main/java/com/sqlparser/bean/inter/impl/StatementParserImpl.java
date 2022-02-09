package com.sqlparser.bean.inter.impl;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.PermissionUtils;
import com.sqlparser.PrintUtils;
import com.sqlparser.SplitUtils;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.FieldName;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.TableName;
import com.sqlparser.bean.inter.StatementParser;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * 解析select类
 */
public abstract class StatementParserImpl implements StatementParser {
    @Override
    public DatabaseName parseDatabase(SQLExprTableSource tableSource, Permission permission) {
        if (tableSource !=null && !StringUtils.isBlank(tableSource.toString())){
            //db.tb || tb
            String db = SplitUtils.getLeft(tableSource.toString());
            if (!StringUtils.isBlank(db)){
                return new DatabaseName(db, PermissionUtils.getPermissionSet(permission));
            }
        } else {
            PrintUtils.printMethodName(tableSource);
        }
        return null;
    }

    @Override
    public TableName parseTableName(SQLExprTableSource tableSource, Permission permission) {
        if (tableSource !=null && !StringUtils.isBlank(tableSource.toString())){
            //db.tb || tb
            String tb = SplitUtils.getRight(tableSource.toString());
            if (!StringUtils.isBlank(tb)){
                return new TableName(tb, PermissionUtils.getPermissionSet(permission));
            }
        } else {
            PrintUtils.printMethodName(tableSource);
        }
        return null;
    }

    @Override
    public FieldName parseFieldName() {
        return null;
    }

    public void parseSQLExprTableSource(SQLExprTableSource sqlExprTableSource, Permission permission){
        DatabaseName databaseName2 = parseDatabase(sqlExprTableSource, permission);
        addDatabaseName(databaseName2);
        TableName tableName2 = parseTableName(sqlExprTableSource, permission);
        addTableName(tableName2);
    }
    public void parseSQLExprTableSourceList(List<SQLExprTableSource> list, Permission permission){
        for (SQLExprTableSource sqlExprTableSource : list){
            parseSQLExprTableSource(sqlExprTableSource, Permission.READ);
        }
    }


    public void addDatabaseName(DatabaseName databaseName){
        DatabaseName dbn = databaseNameMap.getOrDefault(databaseName.getDatabaseName(), new DatabaseName(databaseName.getDatabaseName(), PermissionUtils.getEmptyPermissionSet()));
        dbn.getPermissionSet().addAll(databaseName.getPermissionSet());
        databaseNameMap.put(dbn.getDatabaseName(), dbn);
    }

    public void addTableName(TableName tableName){
        TableName tbn = tableNameMap.getOrDefault(tableName.getTableName(), new TableName(tableName.getTableName(), PermissionUtils.getEmptyPermissionSet()));
        tbn.getPermissionSet().addAll(tableName.getPermissionSet());
        tableNameMap.put(tbn.getTableName(), tbn);
    }
}
