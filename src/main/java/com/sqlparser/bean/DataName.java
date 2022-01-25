package com.sqlparser.bean;

import java.util.HashSet;

public class DataName {
    private HashSet<Permission> permissionSet;

    public DataName(HashSet<Permission> permissionSet) {
        this.permissionSet = permissionSet;
    }

    public HashSet<Permission> getPermissionSet() {
        return permissionSet;
    }

    public void setPermissionSet(HashSet<Permission> permissionSet) {
        this.permissionSet = permissionSet;
    }
}
