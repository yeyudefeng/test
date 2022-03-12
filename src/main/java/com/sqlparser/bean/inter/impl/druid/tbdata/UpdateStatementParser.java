package com.sqlparser.bean.inter.impl.druid.tbdata;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.sqlparser.bean.inter.impl.druid.DruidSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

import java.util.ArrayList;

/**
 * 更新表数据sql。
 */
public class UpdateStatementParser extends DruidStatementParserImpl {
    private SQLUpdateStatement sqlUpdateStatement;
    public UpdateStatementParser(SQLUpdateStatement sqlUpdateStatement) {
        this.sqlUpdateStatement = sqlUpdateStatement;
    }

    public void parse() {
        //认为update的后面接的tablesource需要更新权限，sql后面的where，from等需要读取权限。这里解析略显粗糙。
        SQLTableSource tableSource = sqlUpdateStatement.getTableSource();
        ArrayList<SQLExprTableSource> list = new DruidSqlTableSourceParser(tableSource).getList();
        parseSQLExprTableSourceList(list, Permission.UPDATE);
        SQLTableSource from = sqlUpdateStatement.getFrom();
        ArrayList<SQLExprTableSource> fromList = new DruidSqlTableSourceParser(from).getList();
        parseSQLExprTableSourceList(fromList, Permission.READ);
        SQLExpr where = sqlUpdateStatement.getWhere();
        ArrayList<SQLExprTableSource> whereList = new DruidSqlTableSourceParser(where).getList();
        parseSQLExprTableSourceList(whereList, Permission.READ);
    }
}
