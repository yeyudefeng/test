package com.sqlparser.bean.inter.impl.flink;

import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.bean.TableName;
import com.sqlparser.bean.inter.impl.StatementParserImpl;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * 解析select类
 */
public abstract class FlinkStatementParserImpl extends StatementParserImpl {

    public void parseImmutableList(ImmutableList<String> list, Permission permission){
        DatabaseName databaseName = null;
        TableName tableName = null;
        if (list.size() == 3){
            // catalog.database.table
            databaseName = new DatabaseName(list.get(1), PermissionUtils.getPermissionSet(permission));
            tableName = new TableName(list.get(2), PermissionUtils.getPermissionSet(permission));
        } else if (list.size() == 2){
            // database.table
            databaseName = new DatabaseName(list.get(0), PermissionUtils.getPermissionSet(permission));
            tableName = new TableName(list.get(1), PermissionUtils.getPermissionSet(permission));
        } else if (list.size() == 1){
            // table
            tableName = new TableName(list.get(0), PermissionUtils.getPermissionSet(permission));
        }
        addDatabaseName(databaseName);
        addTableName(tableName);
    }
    public void parseImmutableList(List<ImmutableList<String>> list, Permission permission){
        for (ImmutableList<String> immutableList : list){
            parseImmutableList(immutableList, permission);
        }
    }
}
