package com.qy.shucang.xmind;

import com.qy.shucang.excel.dictionary.PData;

import java.util.ArrayList;

/**
 * XMind读取文件路径参数枚举
 */
public enum XMindEnum {

    PathList(getPathList())
    ;

    XMindEnum(ArrayList<String> type) {
        this.type = type;
    }
    private static ArrayList<String> getPathList(){
        ArrayList<String> list = new ArrayList<>();
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_dim_hr.sql");
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_dwd_hr.sql");
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_dwm_hr.sql");
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_dws_hr.sql");
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_ods_hr.sql");
//        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql\\db_dwd_hr_market.sql");
        list.add("C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\read_process_json.txt");
        return list;
    }

    private ArrayList<String> type;


    public ArrayList<String> getType() {
        return type;
    }

}
