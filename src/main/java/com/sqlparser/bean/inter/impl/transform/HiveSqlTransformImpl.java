package com.sqlparser.bean.inter.impl.transform;

import com.sqlparser.PreDealSqlUtils;
import com.sqlparser.bean.inter.SQLTransformer;

public class HiveSqlTransformImpl implements SQLTransformer {
    @Override
    public String transform(String sql) {
        return PreDealSqlUtils.dealSql(sql);
    }
}
