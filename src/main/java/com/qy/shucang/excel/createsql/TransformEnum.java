package com.qy.shucang.excel.createsql;

import com.qy.shucang.excel.dictionary.PData;

/**
 * 数据字典参数枚举
 */
public enum TransformEnum {
    CAMPUS_MYSQL_INFO_TRANSFORM("jdbc:mysql://10.6.14.7:29962/information_schema?characterEncoding=UTF-8","root","123456",
            new PData("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\databaseandtable.txt",
            "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\odscreatesql.txt"),
            new PData("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\odshivecreatesql.txt",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\odshiveselectsql.txt")
    ),
    HR_MYSQL_INFO_TRANSFORM("jdbc:mysql://10.6.14.11:3306/information_schema?characterEncoding=UTF-8","root","123456",
            new PData("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\hr_databaseandtable.txt",
            "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\hr_odscreatesql.txt"),
            new PData("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\hr_odshivecreatesql.txt",
                    "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\mysqlcreatesql\\hr_odshiveselectsql.txt")
    )
    ;

    TransformEnum(String jdbc, String username, String password, PData pData, PData pData2) {
        this.jdbc = jdbc;
        this.username = username;
        this.password = password;
        this.pData = pData;
        this.pData2 = pData2;
    }

    private String jdbc;
    private String username;
    private String password;
    private PData pData;
    private PData pData2;


    public PData getpData2() {
        return pData2;
    }
    public PData getpData() {
        return pData;
    }

    public String getJdbc() {
        return jdbc;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
