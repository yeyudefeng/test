package com.sqlparser.bean;

import java.util.HashSet;
import java.util.Objects;

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
