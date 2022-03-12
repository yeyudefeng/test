package com.sqlparser.bean.inter.impl.druid.db;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

/**
 * 修改数据库sql
 */
public class AlterDatabaseStatementParser extends DruidStatementParserImpl {
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
