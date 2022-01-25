package com.sqlparser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.sqlparser.bean.DatabaseType;
import com.sqlparser.bean.TestSql;
import com.sqlparser.bean.inter.impl.db.AlterDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.db.CreateDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.db.DropDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.tb.*;
import com.sqlparser.bean.inter.impl.tbdata.DeleteStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.InsertStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.SelectStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.UpdateStatementParser;

import java.util.List;

public class SQLParserUtils {

    public static void parse(String sql, String type){
        DatabaseType databaseType = DatabaseType.valueOf(type.toUpperCase());

        switch (databaseType){
            case HIVE:
            case PRESTO:
            case MYSQL:
                List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, databaseType.getType());
                for (SQLStatement sqlStatement : sqlStatements){
                    if (sqlStatement instanceof SQLInsertStatement){
                        new InsertStatementParser((SQLInsertStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLSelectStatement){
                        new SelectStatementParser((SQLSelectStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLDeleteStatement){
                        new DeleteStatementParser((SQLDeleteStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLUpdateStatement){
                        new UpdateStatementParser((SQLUpdateStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLCreateTableStatement){
                        new CreateTableStatementParser((SQLCreateTableStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLDropTableStatement){
                        new DropTableStatementParser((SQLDropTableStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLTruncateStatement){
                        new TruncateTableStatementParser((SQLTruncateStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLAlterTableStatement){
                        new AlterTableStatementParser((SQLAlterTableStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLCreateDatabaseStatement){
                        new CreateDatabaseStatementParser((SQLCreateDatabaseStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLDropDatabaseStatement){
                        new DropDatabaseStatementParser((SQLDropDatabaseStatement) sqlStatement).parse();
                    } else if (sqlStatement instanceof SQLAlterDatabaseStatement){
                        new AlterDatabaseStatementParser((SQLAlterDatabaseStatement) sqlStatement).parse();
                    }
                }
            case ES:
            case FLINK:
            default:
                return;
        }

    }

    public static void main(String[] args) {
        parse(TestSql.sql2, "hive");
    }
}
