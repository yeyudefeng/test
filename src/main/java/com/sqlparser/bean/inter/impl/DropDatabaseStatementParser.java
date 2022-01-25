package com.sqlparser.bean.inter.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.sqlparser.PermissionUtils;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.Permission;

/**
 * 删除数据库sql
 */
public class DropDatabaseStatementParser extends StatementParserImpl{
    private SQLDropDatabaseStatement sqlDropDatabaseStatement;

    public DropDatabaseStatementParser(SQLDropDatabaseStatement sqlDropDatabaseStatement) {
        this.sqlDropDatabaseStatement = sqlDropDatabaseStatement;
    }

    @Override
    public void parse() {
        SQLExpr database = sqlDropDatabaseStatement.getDatabase();
        addDatabaseName(new DatabaseName(database.toString(), PermissionUtils.getPermissionSet(Permission.DELETE)));
    }
}
