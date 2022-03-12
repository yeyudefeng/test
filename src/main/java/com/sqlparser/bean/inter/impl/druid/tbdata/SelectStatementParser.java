package com.sqlparser.bean.inter.impl.druid.tbdata;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.sqlparser.bean.inter.impl.druid.DruidSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;


import java.util.ArrayList;

/**
 * 查询表数据sql
 */
public class SelectStatementParser extends DruidStatementParserImpl {
    public SQLSelectStatement sqlSelectStatement;
    public SelectStatementParser(SQLSelectStatement sqlSelectStatement) {
        this.sqlSelectStatement = sqlSelectStatement;
    }

    @Override
    public void parse() {
        //认为select后面的都需要读取权限
        ArrayList<SQLExprTableSource> list = new DruidSqlTableSourceParser(sqlSelectStatement.getSelect()).getList();
        parseSQLExprTableSourceList(list, Permission.READ);
    }
}
