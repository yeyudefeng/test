package com.sqlparser.bean.inter.impl.flink.tbdata;

import com.sqlparser.bean.inter.impl.flink.FlinkSqlTableSourceParser;
import com.sqlparser.bean.bean.Permission;
import com.sqlparser.bean.inter.impl.flink.FlinkStatementParserImpl;
import org.apache.flink.sql.parser.ddl.SqlCreateView;

public class FlinkCreateViewStatementParser extends FlinkStatementParserImpl {
    private SqlCreateView sqlCreateView; // insert table
    public FlinkCreateViewStatementParser(SqlCreateView sqlCreateView) {
        this.sqlCreateView = sqlCreateView;
    }

    @Override
    public void parse() {
        //create view table
        parseImmutableList(new FlinkSqlTableSourceParser(sqlCreateView.getViewName()).getImmutableLists(), Permission.CREATE);
        //view query
        parseImmutableList(new FlinkSqlTableSourceParser(sqlCreateView.getQuery()).getImmutableLists(), Permission.READ);
    }
}
