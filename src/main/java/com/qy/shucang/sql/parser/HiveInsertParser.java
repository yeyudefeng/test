package com.qy.shucang.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

import java.util.List;
import java.util.TreeSet;

public class HiveInsertParser {
    public static void main(String[] args) {

        HiveStatementParser hiveStatementParser;
        List<SQLStatement> sqlStatements;
        hiveStatementParser = new HiveStatementParser(InfoSql.line);
        sqlStatements = hiveStatementParser.parseStatementList();
        System.out.println(sqlStatements.toString());
        System.out.println("=======");


    }
}
