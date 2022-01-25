//package com.sqlparser;
//
//import com.alibaba.druid.sql.ast.statement.*;
//
//import java.util.ArrayList;
//
//public class FieldNameParser {
//    private ArrayList<SQLExprTableSource> list = new ArrayList<>();
//
//    public FieldNameParser(SQLSelect query) {
//        parseSQLSelect(query);
//    }
//
//    public ArrayList<SQLExprTableSource> getList() {
//        return list;
//    }
//
//    public void parseSQLSelect(SQLSelect sqlSelect) {
//        SQLSelectQueryBlock queryBlock = sqlSelect.getQueryBlock();
//        parseSQLSelectQueryBlock(queryBlock);
//    }
//
//    public void parseSQLSelectQueryBlock(SQLSelectQueryBlock sqlSelectQueryBlock) {
//        SQLTableSource from = sqlSelectQueryBlock.getFrom();
//        parseSQLTableSource(from);
//    }
//
//    public void parseSQLExprTableSource(SQLExprTableSource sqlExprTableSource) {
//        list.add(sqlExprTableSource);
//
//    }
//    public void parseSQLTableSource(SQLTableSource sqlTableSource) {
//        if (sqlTableSource instanceof SQLExprTableSource) {
//            parseSQLExprTableSource((SQLExprTableSource) sqlTableSource);
//        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
//            SQLSelectQueryBlock queryBlock = ((SQLSubqueryTableSource) sqlTableSource).getSelect().getQueryBlock();
//            parseSQLSelectQueryBlock(queryBlock);
//        } else if (sqlTableSource instanceof SQLUnionQueryTableSource) {
//            SQLUnionQuery union = ((SQLUnionQueryTableSource) sqlTableSource).getUnion();
//            parseSQLUnionQuery(union);
//        } else {
//            PrintUtils.printMethodName(sqlTableSource);
//        }
//    }
//    public void parseSQLSelectQuery(SQLSelectQuery sqlSelectQuery){
//        if (sqlSelectQuery instanceof SQLSelectQueryBlock){
//            parseSQLSelectQueryBlock((SQLSelectQueryBlock) sqlSelectQuery);
//        } else if (sqlSelectQuery instanceof SQLUnionQuery){
//            parseSQLUnionQuery((SQLUnionQuery) sqlSelectQuery);
//        } else {
//            PrintUtils.printMethodName(sqlSelectQuery);
//        }
//    }
//    public void parseSQLUnionQuery(SQLUnionQuery sqlUnionQuery){
//        SQLSelectQuery left = sqlUnionQuery.getLeft();
//        parseSQLSelectQuery(left);
//        SQLSelectQuery right = sqlUnionQuery.getRight();
//        parseSQLSelectQuery(right);
//    }
//}
