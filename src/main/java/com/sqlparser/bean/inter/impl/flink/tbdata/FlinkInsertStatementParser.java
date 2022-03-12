package com.sqlparser.bean.inter.impl.flink.tbdata;

import com.sqlparser.bean.inter.impl.flink.FlinkSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.flink.FlinkStatementParserImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

public class FlinkInsertStatementParser extends FlinkStatementParserImpl {
    private RichSqlInsert richSqlInsert; // insert table
    public FlinkInsertStatementParser(RichSqlInsert richSqlInsert) {
        this.richSqlInsert = richSqlInsert;
    }

    @Override
    public void parse() {
        //insert table
        parseImmutableList(((SqlIdentifier) richSqlInsert.getTargetTable()).names, Permission.WRITE);
        //select from table
        parseImmutableList(new FlinkSqlTableSourceParser(richSqlInsert.getSource()).getImmutableLists(), Permission.READ);
    }
}
