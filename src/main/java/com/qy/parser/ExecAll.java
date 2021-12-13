package com.qy.parser;

import com.qy.parser.bean.EDT;
import com.qy.parser.bean.EProcessData;
import com.qy.parser.impl.processjson.ProjectProcessJsonParserImpl;
import com.qy.parser.impl.transfrom.InitQuerySqlForCreateSqlInTestUtils;
import com.qy.parser.impl.transfrom.TransformParserDwImpl;

import java.util.ArrayList;
import java.util.HashMap;

public class ExecAll {
    public static void main(String[] args) {
        ProjectProcessJsonParserImpl projectProcessJsonParser = new ProjectProcessJsonParserImpl();
        projectProcessJsonParser.exec();
        HashMap<String, ArrayList<EProcessData>> processMap = projectProcessJsonParser.processMap;
        new InitQuerySqlForCreateSqlInTestUtils().exec();
        TransformParserDwImpl transformParserDw = new TransformParserDwImpl();
        transformParserDw.exec();
        HashMap<Integer, EDT> tables = transformParserDw.tables;
        System.out.println("=-----");
    }
}
