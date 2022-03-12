package com.sqlparser.bean.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.sqlparser.bean.bean.DatabaseType;
import com.sqlparser.bean.TestSql;
import com.sqlparser.bean.inter.SQLTransformer;
import com.sqlparser.bean.inter.impl.StatementParserImpl;
import com.sqlparser.bean.inter.impl.druid.db.AlterDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.druid.db.CreateDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.druid.db.DropDatabaseStatementParser;
import com.sqlparser.bean.inter.impl.flink.tbdata.FlinkCreateTableStatementParser;
import com.sqlparser.bean.inter.impl.flink.tbdata.FlinkCreateViewStatementParser;
import com.sqlparser.bean.inter.impl.flink.tbdata.FlinkInsertStatementParser;
import com.sqlparser.bean.inter.impl.druid.tb.*;
import com.sqlparser.bean.inter.impl.druid.tbdata.DeleteStatementParser;
import com.sqlparser.bean.inter.impl.druid.tbdata.InsertStatementParser;
import com.sqlparser.bean.inter.impl.druid.tbdata.SelectStatementParser;
import com.sqlparser.bean.inter.impl.druid.tbdata.UpdateStatementParser;
import com.sqlparser.bean.inter.impl.druid.transform.*;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import java.util.*;


public class SQLParserUtils {
    public static List<StatementParserImpl> parse(String sql, String type) throws SqlParseException {
        DatabaseType databaseType = DatabaseType.valueOf(type.toUpperCase());
        switch (databaseType){
            case FLINK:
                return parseFlink(sql);
            case PRESTO:
            case ES:
            case MYSQL:
            case HIVE:
            case SPARK:
                return parseDruid(sql, databaseType);
        }

        return new ArrayList<>();
    }

    public static void main(String[] args) throws SqlParseException {

        parse(TestSql.sql2, "hive");
//        parse(TestSql.sql, "hive");
//        parseFlink(TestSql.sqlFlink);
    }
    private static ArrayList<StatementParserImpl> parseDruid(String sql, DatabaseType databaseType){
        String sqlTemp = transformSql(sql, databaseType);
        databaseType = transformDatabaseType(sqlTemp, databaseType);
        ArrayList<StatementParserImpl> spList = new ArrayList<>();
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sqlTemp, databaseType.getType());
        for (SQLStatement sqlStatement : sqlStatements){
            StatementParserImpl statementParser = null;
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
            statementParser.parse();
            spList.add(statementParser);
        }
        return spList;
    }
    private static ArrayList<StatementParserImpl> parseFlink(String sql) throws SqlParseException {
        org.apache.calcite.sql.parser.SqlParser.Config parserConfig = org.apache.calcite.sql.parser.SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setLex(Lex.JAVA)
                .setIdentifierMaxLength(Integer.MAX_VALUE)
                .build();
        org.apache.calcite.sql.parser.SqlParser flinkParser = org.apache.calcite.sql.parser.SqlParser.create(sql, parserConfig);
        SqlNodeList sqlNodes = flinkParser.parseStmtList();
        ArrayList<StatementParserImpl> spList = new ArrayList<>();
        Iterator<SqlNode> iterator = sqlNodes.iterator();
        while (iterator.hasNext()){
            SqlNode sqlNode = iterator.next();
            StatementParserImpl statementParser = null;
            if (sqlNode instanceof SqlCreateFunction){
                // do nothing
            } else if (sqlNode instanceof SqlCreateTable){
                statementParser = new FlinkCreateTableStatementParser((SqlCreateTable) sqlNode);
            } else if (sqlNode instanceof SqlCreateView){
                statementParser = new FlinkCreateViewStatementParser((SqlCreateView) sqlNode);
            } else if (sqlNode instanceof RichSqlInsert){
                statementParser = new FlinkInsertStatementParser((RichSqlInsert)sqlNode);

            } else {
                PrintUtils.printMethodName(sqlNode, false);
            }
            if (statementParser != null) {
                statementParser.parse();
            }
            spList.add(statementParser);
        }
        return spList;
    }
    private static DatabaseType transformDatabaseType(String sql, DatabaseType databaseType){
        switch (databaseType){
            case ES:
            case SPARK:
            case PRESTO:
            case MYSQL:
                databaseType = DatabaseType.MYSQL;
                break;
            case FLINK:
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
