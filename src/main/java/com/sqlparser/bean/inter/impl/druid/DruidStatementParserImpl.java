package com.sqlparser.bean.inter.impl.druid;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.utils.PrintUtils;
import com.sqlparser.bean.utils.SplitUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.FieldName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.bean.TableName;
import com.sqlparser.bean.inter.impl.StatementParserImpl;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * 解析select类
 */
public abstract class DruidStatementParserImpl extends StatementParserImpl {
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
            parseSQLExprTableSource(sqlExprTableSource, permission);
        }
    }



}
