package com.qy.parser.utils;

import com.qy.parser.enums.EInfo;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;

public class KeyWordUtils {
    public static HashSet<String> getKeyWords(String path){
        HashSet<String> keyWordSet = new HashSet<>();
        String context = FileUtils.read(path).toLowerCase();
        String[] arr = context.split(EInfo.FILE_SEP);
        for (String keyword : arr){
            if (!StringUtils.isBlank(keyword)){
                keyWordSet.add(keyword);
            }
        }
        return keyWordSet;
    }
    public static Boolean isKeyWord(HashSet<String> keyWordSet, String word){
        return keyWordSet.contains(word.toLowerCase());
    }
}
