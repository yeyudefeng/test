package com.sqlparser.bean.inter.impl.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.sqlparser.bean.utils.PrintUtils;
import com.sqlparser.bean.inter.SqlTableSourceParser;


import java.util.ArrayList;

public class DruidSqlTableSourceParser implements SqlTableSourceParser {
    private ArrayList<SQLExprTableSource> list = new ArrayList<>();

    public DruidSqlTableSourceParser(SQLSelect query) {
        parseSQLSelect(query);
    }
    public DruidSqlTableSourceParser(SQLTableSource sqlTableSource) {
        parseSQLTableSource(sqlTableSource);
    }
    public DruidSqlTableSourceParser(SQLExpr expr) {
        parseSQLExpr(expr);
    }


    public ArrayList<SQLExprTableSource> getList() {
        return list;
    }

    private void parseSQLSelect(SQLSelect sqlSelect) {
        if (sqlSelect == null){
            return;
        }
        SQLSelectQueryBlock queryBlock = sqlSelect.getQueryBlock();
        parseSQLSelectQueryBlock(queryBlock);
    }

    private void parseSQLSelectQueryBlock(SQLSelectQueryBlock sqlSelectQueryBlock) {
        if (sqlSelectQueryBlock == null){
            return;
        }
        SQLTableSource from = sqlSelectQueryBlock.getFrom();
        parseSQLTableSource(from);
        SQLExpr where = sqlSelectQueryBlock.getWhere();
        parseSQLExpr(where);
    }


    // only find database || table
    private void parseSQLExpr(SQLExpr sqlExpr){
        if (sqlExpr == null) {
            return;
        }
        if (sqlExpr instanceof SQLBinaryOpExpr){
            parseSQLBinaryOpExpr((SQLBinaryOpExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLIntegerExpr){
            parseSQLIntegerExpr((SQLIntegerExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLCharExpr){
            parseSQLCharExpr((SQLCharExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLAggregateExpr){
            parseSQLAggregateExpr((SQLAggregateExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLIdentifierExpr){
            parseSQLIdentifierExpr((SQLIdentifierExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLPropertyExpr){
            parseSQLPropertyExpr((SQLPropertyExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLMethodInvokeExpr){
            parseSQLMethodInvokeExpr((SQLMethodInvokeExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLBetweenExpr){
            parseSQLBetweenExpr((SQLBetweenExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLCastExpr){
            parseSQLCastExpr((SQLCastExpr) sqlExpr);
        } else if (sqlExpr instanceof SQLInSubQueryExpr){
            SQLSelect subQuery = ((SQLInSubQueryExpr) sqlExpr).subQuery;
            parseSQLSelect(subQuery);
        } else if (sqlExpr instanceof SQLSelect){
            parseSQLSelect((SQLSelect) sqlExpr);
        } else {
            PrintUtils.printMethodName(sqlExpr);
        }

        }

    private void parseSQLCastExpr(SQLCastExpr sqlCastExpr) {
        //do nothing
    }

    private void parseSQLBetweenExpr(SQLBetweenExpr sqlBetweenExpr) {
        //do nothing
    }

    private void parseSQLMethodInvokeExpr(SQLMethodInvokeExpr sqlMethodInvokeExpr) {
        //do nothing
    }

    private void parseSQLIdentifierExpr(SQLIdentifierExpr sqlIdentifierExpr) {
        //do nothing
    }

    private void parseSQLAggregateExpr(SQLAggregateExpr sqlAggregateExpr) {
        //do nothing
    }

    private void parseSQLCharExpr(SQLCharExpr sqlCharExpr) {
        //do nothing
    }

    private void parseSQLIntegerExpr(SQLIntegerExpr sqlIntegerExpr) {
        //do nothing
    }

    private void parseSQLPropertyExpr(SQLPropertyExpr sqlPropertyExpr){
        //do nothing
    }
    private void parseSQLBinaryOpExpr(SQLBinaryOpExpr sqlBinaryOpExpr){
        SQLExpr left = sqlBinaryOpExpr.getLeft();
        parseSQLExpr(left);
        SQLExpr right = sqlBinaryOpExpr.getRight();
        parseSQLExpr(right);
    }

    private void parseSQLExprTableSource(SQLExprTableSource sqlExprTableSource) {
        list.add(sqlExprTableSource);
    }
    private void parseSQLTableSource(SQLTableSource sqlTableSource) {
        if (sqlTableSource == null){
            return;
        }
        if (sqlTableSource instanceof SQLExprTableSource) {
            parseSQLExprTableSource((SQLExprTableSource) sqlTableSource);
        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSelectQueryBlock queryBlock = ((SQLSubqueryTableSource) sqlTableSource).getSelect().getQueryBlock();
            parseSQLSelectQueryBlock(queryBlock);
        } else if (sqlTableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQuery union = ((SQLUnionQueryTableSource) sqlTableSource).getUnion();
            parseSQLUnionQuery(union);
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLTableSource left = ((SQLJoinTableSource) sqlTableSource).getLeft();
            parseSQLTableSource(left);
            SQLTableSource right = ((SQLJoinTableSource) sqlTableSource).getRight();
            parseSQLTableSource(right);
        } else {
            PrintUtils.printMethodName(sqlTableSource);
        }
    }
    private void parseSQLSelectQuery(SQLSelectQuery sqlSelectQuery){
        if (sqlSelectQuery == null){
            return;
        }
        if (sqlSelectQuery instanceof SQLSelectQueryBlock){
            parseSQLSelectQueryBlock((SQLSelectQueryBlock) sqlSelectQuery);
        } else if (sqlSelectQuery instanceof SQLUnionQuery){
            parseSQLUnionQuery((SQLUnionQuery) sqlSelectQuery);
        } else {
            PrintUtils.printMethodName(sqlSelectQuery);
        }
    }
    private void parseSQLUnionQuery(SQLUnionQuery sqlUnionQuery){
        if (sqlUnionQuery == null){
            return;
        }
        SQLSelectQuery left = sqlUnionQuery.getLeft();
        parseSQLSelectQuery(left);
        SQLSelectQuery right = sqlUnionQuery.getRight();
        parseSQLSelectQuery(right);
    }
}
