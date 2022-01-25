package com.sqlparser;

import com.sqlparser.bean.Permission;

import java.util.HashSet;

public class PermissionUtils {
    public static HashSet<Permission> getPermissionSet(Permission permission){
        HashSet<Permission> set = getEmptyPermissionSet();
        set.add(permission);
        return set;
    }
    public static HashSet<Permission> getEmptyPermissionSet(){
        return new HashSet<>();
    }
    public static HashSet<Permission> addPermission(HashSet<Permission> set, Permission permission){
        set.add(permission);
        return set;
    }

}
