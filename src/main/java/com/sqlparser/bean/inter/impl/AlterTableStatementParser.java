package com.sqlparser.bean.inter.impl;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.DatabaseNameParser;
import com.sqlparser.bean.Permission;

import java.util.ArrayList;

/**
 * 修改表结构sql
 */
public class AlterTableStatementParser extends StatementParserImpl{
    public SQLAlterTableStatement sqlAlterTableStatement;
    public AlterTableStatementParser(SQLAlterTableStatement sqlAlterTableStatement) {
        this.sqlAlterTableStatement = sqlAlterTableStatement;
    }

    @Override
    public void parse() {
        SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
        ArrayList<SQLExprTableSource> list = new DatabaseNameParser(tableSource).getList();
        parseSQLExprTableSourceList(list, Permission.ALTER);
    }
}
