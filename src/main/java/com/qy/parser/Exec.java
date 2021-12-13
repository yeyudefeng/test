package com.qy.parser;

import com.qy.parser.impl.processjson.ProjectProcessJsonParserImpl;
import com.qy.parser.impl.transfrom.TransformParserDwImpl;
import com.qy.parser.impl.transfrom.TransformParserImpl;

public class Exec {
    public static void main(String[] args) {
//        new ProjectProcessJsonParserImpl().exec();
//        new TransformParserImpl().exec();
        new TransformParserDwImpl().exec();
    }
}
