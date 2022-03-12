package com.sqlparser.bean.inter.impl.druid.db;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.sqlparser.bean.utils.PermissionUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

/**
 * 删除数据库sql
 */
public class DropDatabaseStatementParser extends DruidStatementParserImpl {
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
