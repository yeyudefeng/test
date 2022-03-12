package com.sqlparser.bean.inter.impl.flink.tbdata;

import com.sqlparser.bean.inter.impl.flink.FlinkSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.flink.FlinkStatementParserImpl;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

public class FlinkCreateTableStatementParser extends FlinkStatementParserImpl {
    private SqlCreateTable sqlCreateTable; // insert table
    public FlinkCreateTableStatementParser(SqlCreateTable sqlCreateTable) {
        this.sqlCreateTable = sqlCreateTable;
    }

    @Override
    public void parse() {
        //create table name
        parseImmutableList(new FlinkSqlTableSourceParser(sqlCreateTable.getTableName()).getImmutableLists(), Permission.CREATE);
    }
}
