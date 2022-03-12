package com.sqlparser.bean.inter.impl.druid.tbdata;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;

import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.sqlparser.bean.inter.impl.druid.DruidSqlTableSourceParser;
import com.sqlparser.bean.utils.PrintUtils;
import com.sqlparser.bean.bean.DatabaseName;
import com.sqlparser.bean.bean.FieldName;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.bean.TableName;
import com.sqlparser.bean.inter.impl.druid.DruidStatementParserImpl;

import java.util.ArrayList;

/**
 * 删除表数据sql
 */
public class DeleteStatementParser extends DruidStatementParserImpl {
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
        ArrayList<SQLExprTableSource> wehereList = new DruidSqlTableSourceParser(where).getList();
        parseSQLExprTableSourceList(wehereList, Permission.READ);
        SQLTableSource from = sqlDeleteStatement.getFrom();
        ArrayList<SQLExprTableSource> fromList = new DruidSqlTableSourceParser(from).getList();
        parseSQLExprTableSourceList(fromList, Permission.READ);
    }
}
