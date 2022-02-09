package com.sqlparser.bean;


public enum DatabaseType {

    HIVE(1,"hive"),
    MYSQL(2,"mysql"),
    PRESTO(3,"presto"),
    ES(4,"es"),
    FLINK(5,"flink"),
    SPARK(6,"spark"),
    ;

    DatabaseType(Integer index, String type) {
        this.index = index;
        this.type = type;
    }

    private Integer index;
    private String type;

    public Integer getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }
}
