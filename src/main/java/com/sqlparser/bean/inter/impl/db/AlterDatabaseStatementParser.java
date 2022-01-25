package com.sqlparser.bean.inter.impl.db;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.sqlparser.PermissionUtils;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.inter.impl.StatementParserImpl;

/**
 * 修改数据库sql
 */
public class AlterDatabaseStatementParser extends StatementParserImpl {
    private SQLAlterDatabaseStatement sqlAlterDatabaseStatement;
    public AlterDatabaseStatementParser(SQLAlterDatabaseStatement sqlAlterDatabaseStatement) {
        this.sqlAlterDatabaseStatement = sqlAlterDatabaseStatement;
    }

    @Override
    public void parse() {
        SQLName name = sqlAlterDatabaseStatement.getName();
        addDatabaseName(new DatabaseName(name.getSimpleName(), PermissionUtils.getPermissionSet(Permission.CREATE)));
    }
}
