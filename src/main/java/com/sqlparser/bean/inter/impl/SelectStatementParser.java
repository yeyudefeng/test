package com.sqlparser.bean.inter.impl;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.sqlparser.DatabaseNameParser;
import com.sqlparser.bean.Permission;


import java.util.ArrayList;

/**
 * 查询表数据sql
 */
public class SelectStatementParser extends StatementParserImpl {
    public SQLSelectStatement sqlSelectStatement;
    public SelectStatementParser(SQLSelectStatement sqlSelectStatement) {
        this.sqlSelectStatement = sqlSelectStatement;
    }

    @Override
    public void parse() {
        //认为select后面的都需要读取权限
        ArrayList<SQLExprTableSource> list = new DatabaseNameParser(sqlSelectStatement.getSelect()).getList();
        parseSQLExprTableSourceList(list, Permission.READ);
    }
}
