package com.sqlparser.bean.inter.impl.druid.tb;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

/**
 * 创建表sql
 */
public class CreateTableStatementParser extends DruidStatementParserImpl {
    private SQLCreateTableStatement sqlCreateTableStatement;
    public CreateTableStatementParser(SQLCreateTableStatement sqlCreateTableStatement) {
        this.sqlCreateTableStatement = sqlCreateTableStatement;
    }

    @Override
    public void parse() {
        //需要库，表具有创建权限
        SQLExprTableSource tableSource = sqlCreateTableStatement.getTableSource();
        parseSQLExprTableSource(tableSource, Permission.CREATE);
    }
}
