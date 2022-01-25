package com.sqlparser.bean.inter;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.sqlparser.bean.DatabaseName;
import com.sqlparser.bean.FieldName;
import com.sqlparser.bean.Permission;
import com.sqlparser.bean.TableName;

import java.util.HashMap;

public interface StatementParser {
    public HashMap<String, DatabaseName> databaseNameMap = new HashMap<>();
    public HashMap<String, TableName> tableNameMap = new HashMap<>();
    public HashMap<String, FieldName> fieldNameMap = new HashMap<>();
    public DatabaseName parseDatabase(SQLExprTableSource tableSource, Permission permission);
    public TableName parseTableName(SQLExprTableSource tableSource, Permission permission);
    public FieldName parseFieldName();
    public void parse();
}
