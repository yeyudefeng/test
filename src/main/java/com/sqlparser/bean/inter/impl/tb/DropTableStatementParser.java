package com.sqlparser.bean.inter.impl.tb;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.DatabaseNameParser;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.inter.impl.StatementParserImpl;

import java.util.List;

/**
 * 删除表sql
 */
public class DropTableStatementParser extends StatementParserImpl {
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
