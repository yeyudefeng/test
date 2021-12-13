package com.qy.shucang.xmind;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.qy.shucang.excel.dictionary.Data;
import com.qy.shucang.excel.dictionary.FileUtils;
import com.qy.shucang.excel.dictionary.ProjectProcessJsonParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

public class XMindParserDao implements XMindParser{
    private ArrayList<String> list = new ArrayList<String>();
    private ArrayList<Data> dataList = new ArrayList<Data>();
    @Override
    public void open() {
        for (String path : XMindEnum.PathList.getType()){
            list.add(FileUtils.read(path));
        }
    }

    @Override
    public void parser() {
        ProjectProcessJsonParser projectProcessJsonParser = new ProjectProcessJsonParser();
        projectProcessJsonParser.exec();
        dataList = projectProcessJsonParser.getDataList();
        ArrayList<FT> fts = new ArrayList<>();
        for (Data data : dataList){
            try {
                if (!data.processName.equalsIgnoreCase("db_ods_hr")){
                    if (data.sql != null && data.sql.contains("create ") && data.sql.contains("select ") && data.sql.contains("where ")){
                        fts.add(parseCreateSelectSql(data.sql));
                    } else if (data.sql != null && data.sql.startsWith("insert ")){
                        fts.add(parseInsertSql(data.sql));
                    }
                } else {

                }
                System.out.println("======");
            } catch (Exception e){
                e.printStackTrace();
                System.out.println("catch error " + data.name + " ===== " + data.sql);
            }
            System.out.println("======");

        }
    }

    private FT parseInsertSql(String sql) throws SemanticException, ParseException {
        LineageInfo lep = new LineageInfo();
        lep.getLineageInfo(sql);
        TreeSet<String> inputTableList = lep.getInputTableList();
        TreeSet<String> outputTableList = lep.getOutputTableList();
        HashSet<String> hashSet = new HashSet<>();
        hashSet.addAll(inputTableList);
        return new FT(outputTableList.first(), hashSet);
    }

    private FT parseCreateSelectSql(String sql) throws SemanticException, ParseException {
        LineageInfo lep = new LineageInfo();
        lep.getLineageInfo(sql);
        TreeSet<String> inputTableList = lep.getInputTableList();
        TreeSet<String> outputTableList = lep.getOutputTableList();
        HiveStatementParser hiveStatementParser;
        List<SQLStatement> sqlStatements;
        hiveStatementParser = new HiveStatementParser(sql);
        sqlStatements = hiveStatementParser.parseStatementList();
        SQLExprTableSource tableSource = ((HiveCreateTableStatement) sqlStatements.get(0)).getTableSource();
        HashSet<String> hashSet = new HashSet<>();
        hashSet.addAll(inputTableList);
        return new FT(tableSource.toString(), hashSet);

    }

    private void parseSqlStat(SQLStatement sqlStatement) {
        if (sqlStatement instanceof HiveCreateTableStatement){
//            ((HiveCreateTableStatement) sqlStatement)
            SQLTableSource from = ((HiveCreateTableStatement) sqlStatement).getSelect().getQueryBlock().getFrom();
            System.out.println("ssd");
        }
    }

    @Override
    public void exec() {
        open();
        parser();
        close();
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        new XMindParserDao().exec();
    }


}
class FT{
    public String f;
    public HashSet<String> t;

    public FT(String f, HashSet<String> t) {
        this.f = f;
        this.t = t;
    }
}