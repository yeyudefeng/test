package com.sqlparser.bean.inter.impl.druid.transform;

import com.sqlparser.bean.utils.PreDealSqlUtils;
import com.sqlparser.bean.inter.SQLTransformer;

public class HiveSqlTransformImpl implements SQLTransformer {
    @Override
    public String transform(String sql) {
        return PreDealSqlUtils.dealSql(sql);
    }
}
