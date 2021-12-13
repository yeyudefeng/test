package com.qy.shucang.excel.createsql;

public interface Transformer {
    public void open();
    public void transformer();
    public void exec();
    public void close();
}
