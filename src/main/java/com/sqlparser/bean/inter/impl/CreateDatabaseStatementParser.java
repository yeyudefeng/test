package com.sqlparser.bean.inter.impl;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.sqlparser.PermissionUtils;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.Permission;

/**
 * 创建数据库sql
 */
public class CreateDatabaseStatementParser extends StatementParserImpl{
    private SQLCreateDatabaseStatement sqlCreateDatabaseStatement;
    public CreateDatabaseStatementParser(SQLCreateDatabaseStatement sqlCreateDatabaseStatement) {
        this.sqlCreateDatabaseStatement = sqlCreateDatabaseStatement;
    }

    @Override
    public void parse() {
        SQLName name = sqlCreateDatabaseStatement.getName();
        addDatabaseName(new DatabaseName(name.getSimpleName(), PermissionUtils.getPermissionSet(Permission.CREATE)));
    }
}
