package com.sqlparser.bean.inter.impl.druid.transform;

import com.sqlparser.bean.inter.SQLTransformer;

public class MysqlSqlTransformImpl implements SQLTransformer {
    @Override
    public String transform(String sql) {
        return sql;
    }
}
