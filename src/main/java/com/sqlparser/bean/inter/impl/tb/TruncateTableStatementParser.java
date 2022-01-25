package com.sqlparser.bean.inter.impl.tb;

import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.inter.impl.StatementParserImpl;

import java.util.List;

/**
 * 删除表sql
 */
public class TruncateTableStatementParser extends StatementParserImpl {
    private SQLTruncateStatement sqlTruncateStatement;
    public TruncateTableStatementParser(SQLTruncateStatement sqlTruncateStatement) {
        this.sqlTruncateStatement = sqlTruncateStatement;
    }

    @Override
    public void parse() {
        //需要库，表具有删除权限
        List<SQLExprTableSource> tableSources = sqlTruncateStatement.getTableSources();
        parseSQLExprTableSourceList(tableSources, Permission.DELETE);
    }
}
