package com.sqlparser.bean.inter.impl.tbdata;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;

import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.sqlparser.DatabaseNameParser;
import com.sqlparser.PrintUtils;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.FieldName;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.TableName;
import com.sqlparser.bean.inter.impl.StatementParserImpl;

import java.util.ArrayList;

/**
 * 删除表数据sql
 */
public class DeleteStatementParser extends StatementParserImpl {
    private SQLDeleteStatement sqlDeleteStatement; // insert table

    public DeleteStatementParser(SQLDeleteStatement sqlDeleteStatement) {
        this.sqlDeleteStatement = sqlDeleteStatement;
    }

    @Override
    public DatabaseName parseDatabase(SQLExprTableSource tableSource, Permission permission) {
        return null;
    }

    @Override
    public TableName parseTableName(SQLExprTableSource tableSource, Permission permission) {
        return null;
    }

    @Override
    public FieldName parseFieldName() {
        return null;
    }

    @Override
    public void parse() {
        //认为delete后面的库，表，需要同时具备读取，删除权限才可以执行删除sql，肖老师认为不需要判断读取权限。
        //认为在delete中遇到的 select || in subquery || join || where || from 中遇到的表都需要读取权限。
        SQLTableSource tableSource = sqlDeleteStatement.getTableSource();
        if (tableSource instanceof SQLExprTableSource){

//            parseSQLExprTableSource((SQLExprTableSource) tableSource, Permission.READ);
            parseSQLExprTableSource((SQLExprTableSource) tableSource, Permission.DELETE);
        } else {
            PrintUtils.printMethodName(tableSource);
        }
        SQLExpr where = sqlDeleteStatement.getWhere();
        ArrayList<SQLExprTableSource> wehereList = new DatabaseNameParser(where).getList();
        parseSQLExprTableSourceList(wehereList, Permission.READ);
        SQLTableSource from = sqlDeleteStatement.getFrom();
        ArrayList<SQLExprTableSource> fromList = new DatabaseNameParser(from).getList();
        parseSQLExprTableSourceList(fromList, Permission.READ);
    }
}
