package com.sqlparser.bean.inter.impl.druid.db;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

/**
 * 创建数据库sql
 */
public class CreateDatabaseStatementParser extends DruidStatementParserImpl {
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
