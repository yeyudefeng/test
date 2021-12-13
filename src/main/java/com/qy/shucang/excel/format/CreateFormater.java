package com.qy.shucang.excel.format;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.qy.shucang.excel.dictionary.ExcelInfo;
import com.qy.shucang.excel.dictionary.ExcelSheet;
import com.qy.shucang.excel.dictionary.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 按行处理，格式化字段注释
 * 格式化create sql
 */

public class CreateFormater implements Formater{
    public String sep = FileUtils.sep;
    public String readPath;
    public String writePath;
    public String context;
    private Integer len = 30;
    public StringBuffer sb = new StringBuffer();
    private FormaterDDLEnum formaterEnum;
    public void open() {
        readPath = formaterEnum.getReadPath();
        writePath = formaterEnum.getWritePath();
    }

    public CreateFormater(FormaterDDLEnum formaterEnum) {
        this.formaterEnum = formaterEnum;
    }

    public void format() {
        initContext();
        String[] arr = context.replaceAll("-{3,100}", "=================")
                .replaceAll("\t","    ").split(sep);

        for (int i = 0; i < arr.length; i++){
            String line = arr[i].trim();
            if (!StringUtils.isBlank(line)
                    && !line.startsWith("create table ")
                    && !line.startsWith(")")
                    && !line.startsWith("partitioned by")
                    && !line.startsWith("row format ")
                    && !line.startsWith("stored as ")
                    && !line.startsWith(";")
                    && !line.startsWith("=")
                    && !line.startsWith("-")
            ){
                Field field = parseLine(line);
                Integer max = len;
                while (max < field.l.length()){
                    max += 20;
                }
                sb.append("    ," + StringUtils.rightPad(field.l, max + 2, " ")
                        + StringUtils.rightPad(field.m, len - 10, " "));
                if (!StringUtils.isBlank(field.r)){
                    sb.append(" comment " + field.r);
                }
                sb.append(sep);
            } else {
                sb.append(line + sep);
            }
        }
        StringBuffer ssb = new StringBuffer();
        String[] sqls = sb.toString().split("create table ");
        for (String sql : sqls){
            if (!StringUtils.isBlank(sql)){
                sql = "create table " + sql;
                sql = sql.replaceFirst(","," ");
            }
            ssb.append(sql);
        }
        FileUtils.write(writePath, ssb.toString());
    }



    private Field parseLine(String line) {
        Integer max = len;
        String comment;
        String field;
        String type;
        String[] arr = line.split(" comment ");
        if (arr.length == 1){
            comment = "";
        } else {
            String ss = arr[1].trim();
            comment = ss.endsWith(",") ? ss.substring(0,ss.length() -1) : ss;
        }
        String[] sp = arr[0].split(" {1,100}");
        if (sp.length >= 2){
            field = sp[0].startsWith(",") ? sp[0].substring(1) : sp[0];
            type = sp[1];
        } else {
            throw new RuntimeException("field catch some error, " + line);
        }
        return new Field(field, type, comment);
    }


    public void close() {
        System.out.println("task is success");
    }

    public void exec() {
        open();
        format();
        close();
    }
    public  void initContext(){
        context = FileUtils.read(readPath).toLowerCase();
    }

}
