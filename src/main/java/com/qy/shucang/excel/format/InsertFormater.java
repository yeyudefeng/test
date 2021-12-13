package com.qy.shucang.excel.format;

import com.qy.shucang.excel.dictionary.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * 按行处理，格式化字段注释
 * 格式化select / insert sql
 */

public class InsertFormater implements Formater{
    public String sep = FileUtils.sep;
    public String readPath;
    public String writePath;
    public String context;
    private Integer len = 50;
    public StringBuffer sb = new StringBuffer();
    private FormaterDMLEnum formaterEnum;
    public void open() {
        readPath = formaterEnum.getReadPath();
        writePath = formaterEnum.getWritePath();
    }

    public InsertFormater(FormaterDMLEnum formaterEnum) {
        this.formaterEnum = formaterEnum;
    }

    public void format() {
        initContext();
        String[] arr = context.replaceAll("-{3,100}", "=================").replaceAll("`","")
                .replaceAll("\t","    ").split(sep);

        for (int i = 0; i < arr.length; i++){
            String line = arr[i];
            if (!StringUtils.isBlank(line) && line.contains("--") && !line.trim().startsWith("--")){
                parseLine(line);
            } else {
                sb.append(line + sep);
            }
        }
        FileUtils.write(writePath, sb.toString());
    }

    private void parseLine(String line) {
        Integer max = len;
        String[] arr = line.split("--");
        if (arr.length >= 2){
            String left = arr[0].replaceAll(" {1,100}$","");
            String right = " " + line.substring(arr[0].length() + 2).trim();
            while (max < left.length()){
                max += 20;
            }
            sb.append(StringUtils.rightPad(left, max + 2, " ") + " -- " + right + sep);
        } else {
            throw new RuntimeException("sql some line contains two '--' : " + line);
        }
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
