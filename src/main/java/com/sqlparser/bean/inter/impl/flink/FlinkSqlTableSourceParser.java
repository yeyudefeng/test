package com.sqlparser.bean.inter.impl.flink;


import com.sqlparser.bean.utils.PrintUtils;
import com.sqlparser.bean.inter.SqlTableSourceParser;
import org.apache.calcite.sql.*;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;

/**
 * 构造器只传table or sqlselect
 */
public class FlinkSqlTableSourceParser implements SqlTableSourceParser {
    private ArrayList<ImmutableList<String>> immutableLists = new ArrayList<>();
    public FlinkSqlTableSourceParser(SqlNode sqlNode) {
        parseSqlSelect(sqlNode);
    }

    public FlinkSqlTableSourceParser(SqlIdentifier sqlIdentifier) {
        parseSqlSelect(sqlIdentifier);
    }


    public ArrayList<ImmutableList<String>> getImmutableLists() {
        return immutableLists;
    }

    private void parseSqlSelect(SqlNode sqlNode) {
        if (sqlNode == null) {
            return;
        }
        if (sqlNode instanceof SqlSelect) {
            parseSqlSelect(((SqlSelect)sqlNode).getFrom());
        } else if (sqlNode instanceof SqlIdentifier){
            parseSqlIdentifier((SqlIdentifier)sqlNode);
        } else if (sqlNode instanceof SqlBasicCall){
            parseSqlBasicCall((SqlBasicCall)sqlNode);
        } else if (sqlNode instanceof SqlJoin){
            parseSqlJoin((SqlJoin)sqlNode);
        } else {
            System.out.println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww");
            PrintUtils.printMethodName(sqlNode, false);
            System.out.println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww");
        }
    }

    private void parseSqlJoin(SqlJoin sqlJoin) {
        SqlNode left = sqlJoin.getLeft();
        parseSqlSelect(left);
        SqlNode right = sqlJoin.getRight();
        parseSqlSelect(right);
    }

    private void parseSqlBasicCall(SqlBasicCall sqlBasicCall) {
        SqlOperator operator = sqlBasicCall.getOperator();
        if (operator instanceof SqlAsOperator){
            SqlNode operand = sqlBasicCall.operand(0);
            parseSqlSelect(operand);
        }
        System.out.println();
    }


    private void parseSqlIdentifier(SqlIdentifier sqlIdentifier) {
        ImmutableList<String> names = sqlIdentifier.names;
        immutableLists.add(names);
    }
}
