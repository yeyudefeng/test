package com.qy.shucang.replace;

import com.qy.shucang.excel.dictionary.FileUtils;

public class ReplacePartdateParser implements ReplaceParser{
    public String context;
    public String result;

    /**
     * 初始化读取文件
     */
    @Override
    public void open() {
        context = FileUtils.read(ReplaceEnum.READ_PATH.getValue()).toLowerCase();
    }

    /**
     * 正则替换，写入文件
     */
    @Override
    public void replace() {
        result = context.replaceAll(ReplaceEnum.PARTDATE_REGEXP.getValue(), ReplaceEnum.SYSTEM_BIZ_DATE.getValue());
        FileUtils.write(ReplaceEnum.WRITE_PATH.getValue(), result);
    }

    @Override
    public void exec() {
        open();
        replace();
        close();
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {
        new ReplacePartdateParser().exec();
    }
}
