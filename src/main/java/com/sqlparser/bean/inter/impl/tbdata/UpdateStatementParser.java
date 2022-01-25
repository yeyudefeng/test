package com.sqlparser.bean.inter.impl.tbdata;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.sqlparser.DatabaseNameParser;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.inter.impl.StatementParserImpl;

import java.util.ArrayList;

/**
 * 更新表数据sql。
 */
public class UpdateStatementParser extends StatementParserImpl {
    private SQLUpdateStatement sqlUpdateStatement;
    public UpdateStatementParser(SQLUpdateStatement sqlUpdateStatement) {
        this.sqlUpdateStatement = sqlUpdateStatement;
    }

    public void parse() {
        //认为update的后面接的tablesource需要更新权限，sql后面的where，from等需要读取权限。这里解析略显粗糙。
        SQLTableSource tableSource = sqlUpdateStatement.getTableSource();
        ArrayList<SQLExprTableSource> list = new DatabaseNameParser(tableSource).getList();
        parseSQLExprTableSourceList(list, Permission.UPDATE);
        SQLTableSource from = sqlUpdateStatement.getFrom();
        ArrayList<SQLExprTableSource> fromList = new DatabaseNameParser(from).getList();
        parseSQLExprTableSourceList(fromList, Permission.READ);
        SQLExpr where = sqlUpdateStatement.getWhere();
        ArrayList<SQLExprTableSource> whereList = new DatabaseNameParser(where).getList();
        parseSQLExprTableSourceList(whereList, Permission.READ);
    }
}
