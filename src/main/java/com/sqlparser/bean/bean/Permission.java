package com.sqlparser.bean.bean;

public enum  Permission {
    READ(1),//读取
    WRITE(2),//写入
    UPDATE(3),//更新数据
    DELETE(4),//删除
    ALTER(5),//修改结构
    CREATE(6),//创建
    ;
    private int permission;

    Permission(int permission) {
        this.permission = permission;
    }
}
