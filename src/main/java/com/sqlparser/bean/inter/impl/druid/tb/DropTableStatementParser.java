package com.sqlparser.bean.inter.impl.druid.tb;

import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

import java.util.List;

/**
 * 删除表sql
 */
public class DropTableStatementParser extends DruidStatementParserImpl {
    private SQLDropTableStatement sqlDropTableStatement;
    public DropTableStatementParser(SQLDropTableStatement sqlDropTableStatement) {
        this.sqlDropTableStatement = sqlDropTableStatement;
    }

    @Override
    public void parse() {
        //需要库，表具有删除权限
        List<SQLExprTableSource> tableSources = sqlDropTableStatement.getTableSources();
        parseSQLExprTableSourceList(tableSources, Permission.DELETE);
    }
}
