package com.qy.parser.impl.processjson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qy.parser.bean.EProcessData;
import com.qy.parser.bean.ETask;
import com.qy.parser.bean.ProcessDefinition;
import com.qy.parser.impl.transfrom.LevelUtils;
import com.qy.parser.interfaces.ProjectProcessJsonParser;
import com.qy.parser.utils.FileUtils;
import org.apache.commons.lang.StringUtils;


import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

import static com.qy.parser.enums.EInfo.*;

public class ProjectProcessJsonParserImpl implements ProjectProcessJsonParser {
    private String context;
    public HashMap<String, ArrayList<EProcessData>> processMap = new HashMap<>();
    @Override
    public void parser() {
        JSONObject json = JSON.parseObject(context);
        JSONArray data = json.getJSONObject(JSON_DATA).getJSONArray(JSON_TOTAL_LIST);
        for (String processName : PROCESS_NAME_ARR){
            ArrayList<EProcessData> list = new ArrayList<>();
            for (int i = 0; i< data.size(); i ++){
                ProcessDefinition pd = JSON.parseObject(data.get(i).toString(), ProcessDefinition.class);
                if (processName.equalsIgnoreCase(pd.getName())){
                    String processDefinitionJson = pd.getProcessDefinitionJson();
                    JSONArray tasks = JSON.parseObject(processDefinitionJson).getJSONArray(JSON_TASKS);
                    for (int j = 0; j < tasks.size(); j++){
                        ETask task = JSON.parseObject(tasks.get(j).toString(), ETask.class);
                        EProcessData eProcessData = parseTask(task, processName);
                        if (eProcessData != null) {
                            list.add(eProcessData);
                        }
                    }
                }
            }
            processMap.put(processName, list);
            for (EProcessData eProcessData : list){
                insertEachSqlToFile(eProcessData);
            }
        }
        if (IS_WRITE_PROCESS_JSON){
            writeFile();
        }
    }

    private void insertEachSqlToFile(EProcessData eProcessData) {
        String sql = eProcessData.sql;
        String fileName = "";
        String level = "";
        String fullTableName;
        String tableName;
        if (sql.trim().toLowerCase().startsWith("select ")){
            level = ODS;
            tableName = eProcessData.name;
        } else if (sql.trim().toLowerCase().startsWith("insert ")){
            fullTableName = sql.toLowerCase().split(" table ")[1].trim().split("\n")[0].split("\\(")[0].split(" ")[0];
            level = LevelUtils.getLevel(fullTableName);
            tableName = fullTableName.split("\\.")[1];
        } else {
            throw new RuntimeException( " sql not start with select or insert , please check sql");
        }
//        fileName = SHUCANG_PATH + "\\parser\\separate\\" + SHUCANG_NAME + UNDERLINE + level + UNDERLINE + "导数";
        fileName = SHUCANG_PATH + "\\parser\\separate\\" + SHUCANG_NAME + BACKSLASH + level + BACKSLASH + "导数";
        File file = new File(fileName);
        if (!file.exists()){
            file.mkdirs();
        }
        FileUtils.write(file.getAbsolutePath() + "\\" + tableName + ".sql", sql);
    }

    @Override
    public void open() {
        context = FileUtils.read(PROCESS_JSON_EDATA.rp);
    }

    @Override
    public void exec() {
        open();
        parser();
        close();
    }

    @Override
    public void close() {

    }
    public Boolean writeFile(){
        for (String processName : processMap.keySet()){
            StringBuffer sb = new StringBuffer();
            for (EProcessData eProcessData : processMap.get(processName)){
                sb.append(format(eProcessData));
            }
            FileUtils.write(PROCESS_JSON_EDATA.wp + "\\" + processName + FILE_SUFFIX_SQL, sb.toString());
        }
        return true;
    }


    private StringBuffer format(EProcessData eProcessData){
        return new StringBuffer().append(StringUtils.rightPad("", MAX_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad(StringUtils.rightPad("", MIN_LEN, "=") + StringUtils.rightPad("", MIN_LEN, " ") + eProcessData.name, MAX_LEN - MIN_LEN, " ") + StringUtils.rightPad("", MIN_LEN, "=") + FILE_SEP)
                .append(StringUtils.rightPad("", MAX_LEN, "=") + FILE_SEP)
                .append(eProcessData.sql + FILE_SEP + FILE_SEP + FILE_SEP)
                ;
    }

    private EProcessData parseTask(ETask task, String processName) {
        EProcessData data = null;
        if (task.type.equalsIgnoreCase(DATAX_TYPE)){
            data = parseDataxTask(task);
            data.processName = processName;
        } else if (task.type.equalsIgnoreCase(SQL_TYPE)){
            data = parseSqlTask(task);
            data.processName = processName;
        }
        return data;
    }
    private EProcessData parseSqlTask(ETask task) {
        String sql = JSONObject.parseObject(task.params).getString(JSON_SQL);
        return new EProcessData(task.name, sql);
    }

    private EProcessData parseDataxTask(ETask task) {
        String sql = JSONObject.parseObject(task.params).getString(JSON_SQL);
        return new EProcessData(task.name, sql);
    }
}
