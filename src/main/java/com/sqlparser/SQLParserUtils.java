package com.sqlparser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.sqlparser.bean.DatabaseType;
import com.sqlparser.bean.TestSql;
import com.sqlparser.bean.inter.SQLTransformer;
import com.sqlparser.bean.inter.StatementParser;
import com.sqlparser.bean.inter.impl.db.AlterDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.db.CreateDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.db.DropDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.tb.*;
import com.sqlparser.bean.inter.impl.tbdata.DeleteStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.InsertStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.SelectStatementParser;
import com.sqlparser.bean.inter.impl.tbdata.UpdateStatementParser;
import com.sqlparser.bean.inter.impl.transform.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SQLParserUtils {

    public static List<StatementParser> parse(String sql, String type){
        DatabaseType databaseType = DatabaseType.valueOf(type.toUpperCase());
        String sqlTemp = transformSql(sql, databaseType);
        databaseType = transformDatabaseType(sqlTemp, databaseType);
        ArrayList<StatementParser> spList = new ArrayList<>();
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sqlTemp, databaseType.getType());
        for (SQLStatement sqlStatement : sqlStatements){
            StatementParser statementParser = null;
            if (sqlStatement instanceof SQLInsertStatement){
                statementParser = new InsertStatementParser((SQLInsertStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLSelectStatement){
                statementParser = new SelectStatementParser((SQLSelectStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLDeleteStatement){
                statementParser = new DeleteStatementParser((SQLDeleteStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLUpdateStatement){
                statementParser = new UpdateStatementParser((SQLUpdateStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLCreateTableStatement){
                statementParser = new CreateTableStatementParser((SQLCreateTableStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLDropTableStatement){
                statementParser = new DropTableStatementParser((SQLDropTableStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLTruncateStatement){
                statementParser = new TruncateTableStatementParser((SQLTruncateStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLAlterTableStatement){
                statementParser = new AlterTableStatementParser((SQLAlterTableStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLCreateDatabaseStatement){
                statementParser = new CreateDatabaseStatementParser((SQLCreateDatabaseStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLDropDatabaseStatement){
                statementParser = new DropDatabaseStatementParser((SQLDropDatabaseStatement) sqlStatement);
            } else if (sqlStatement instanceof SQLAlterDatabaseStatement){
                statementParser = new AlterDatabaseStatementParser((SQLAlterDatabaseStatement) sqlStatement);
            } else {
                PrintUtils.printMethodName(sqlStatement, false);
            }
            spList.add(statementParser);
        }
        return spList.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        parse(TestSql.sql2, "hive");
    }

    private static DatabaseType transformDatabaseType(String sql, DatabaseType databaseType){
        switch (databaseType){
            case ES:
            case FLINK:
            case SPARK:
            case PRESTO:
                databaseType = DatabaseType.MYSQL;
                break;
            case MYSQL:
            case HIVE:
            default:
                break;
        }
        return databaseType;
    }
    private static String transformSql(String sql, DatabaseType databaseType){
        SQLTransformer sqlTransformer;
        switch (databaseType){
            case HIVE:
                sqlTransformer = new HiveSqlTransformImpl();
                break;
            case PRESTO:
                sqlTransformer = new PrestoSqlTransformImpl();
                break;
            case ES:
                sqlTransformer = new ESSqlTransformImpl();
                break;
            case FLINK:
                sqlTransformer = new FlinkSqlTransformImpl();
                break;
            case SPARK:
                sqlTransformer = new SparkSqlTransformImpl();
                break;
            case MYSQL:
            default:
                sqlTransformer = new MysqlSqlTransformImpl();
                break;
        }
        return sqlTransformer.transform(sql);
    }
}
