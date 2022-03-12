package com.sqlparser.bean.bean;

import java.util.HashSet;

public class FieldName  extends DataName{
    public String fieldName;

    public FieldName(String fieldName, HashSet<Permission> permissionSet) {
        super(permissionSet);
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
