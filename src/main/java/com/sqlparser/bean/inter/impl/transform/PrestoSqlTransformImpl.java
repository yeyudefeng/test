package com.sqlparser.bean.inter.impl.transform;

import com.sqlparser.bean.inter.SQLTransformer;

public class PrestoSqlTransformImpl implements SQLTransformer {
    @Override
    public String transform(String sql) {
        return sql;
    }
}
