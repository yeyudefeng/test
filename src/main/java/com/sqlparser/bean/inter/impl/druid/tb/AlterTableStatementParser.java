package com.sqlparser.bean.inter.impl.druid.tb;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.bean.inter.impl.druid.DruidSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

import java.util.ArrayList;

/**
 * 修改表结构sql
 */
public class AlterTableStatementParser extends DruidStatementParserImpl {
    public SQLAlterTableStatement sqlAlterTableStatement;
    public AlterTableStatementParser(SQLAlterTableStatement sqlAlterTableStatement) {
        this.sqlAlterTableStatement = sqlAlterTableStatement;
    }

    @Override
    public void parse() {
        SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
        ArrayList<SQLExprTableSource> list = new DruidSqlTableSourceParser(tableSource).getList();
        parseSQLExprTableSourceList(list, Permission.ALTER);
    }
}
