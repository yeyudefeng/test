package com.sqlparser.bean.inter.impl.druid.tbdata;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.sqlparser.bean.inter.impl.druid.DruidSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

import java.util.ArrayList;

/**
 * 插入表数据sql
 */
public class InsertStatementParser extends DruidStatementParserImpl {
    private SQLInsertStatement sqlInsertStatement; // insert table

    public InsertStatementParser(SQLInsertStatement sqlInsertStatement) {
        this.sqlInsertStatement = sqlInsertStatement;
    }

    @Override
    public void parse() {
        //认为insert table需要写入权限，select 需要读取权限
        parseSQLExprTableSource(sqlInsertStatement.getTableSource(), Permission.WRITE);
        ArrayList<SQLExprTableSource> list = new DruidSqlTableSourceParser(sqlInsertStatement.getQuery()).getList();
        parseSQLExprTableSourceList(list, Permission.READ);
    }
}
