package com.qy.shucang.excel.dictionary;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qy.parser.enums.EInfo;

import java.util.ArrayList;

/**
 * 解析从平台获取的大json，解析出对应的insert / select sql，等待后续处理。
 */
public class ProjectProcessJsonParser implements ExcelParser{
    private String filePath = EInfo.READ_PROCESS_JSON_FILE;
    private String writeFilePath = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources\\hr\\insertsql";
    private String context;
    private StringBuffer sb = new StringBuffer();
    private ArrayList<Data> dataList = new ArrayList<Data>();

    public void open() {

    }

    public ArrayList<Data> getDataList() {
        return dataList;
    }


//    private String[] processNameArr = new String[]{"dwh_campus_ods", "dwh_campus_dwd", "dwh_campus_dws", "dwh_campus_dim"};
    private String[] processNameArr = EInfo.PROCESS_NAME_ARR;
    public void parser() {
        context = FileUtils.read(filePath);
        JSONObject json = JSON.parseObject(context);
//        JSONArray data = json.getJSONArray("data");
        JSONArray data = json.getJSONObject("data").getJSONArray("totalList");
        for (String processName : processNameArr){
            for (int i = 0; i< data.size(); i ++){
                ProcessDefinition pd = JSON.parseObject(data.get(i).toString(), ProcessDefinition.class);
                if (processName.equalsIgnoreCase(pd.getName())){
                    String processDefinitionJson = pd.getProcessDefinitionJson();
                    JSONArray tasks = JSON.parseObject(processDefinitionJson).getJSONArray("tasks");
                    for (int j = 0; j < tasks.size(); j++){
                        Task task = JSON.parseObject(tasks.get(j).toString(), Task.class);
                        parseTask(task, processName);
                    }
                }
            }
            FileUtils.write(writeFilePath + "\\" + processName + ".sql", sb.toString());
            sb = new StringBuffer();
        }
    }

    private void parseTask(Task task, String processName) {
        Data data = null;
        if (task.type.equalsIgnoreCase("datax")){
            data = parseDataxTask(task);
            data.processName = processName;
        } else if (task.type.equalsIgnoreCase("sql")){
            data = parseSqlTask(task);
            data.processName = processName;
        }
        if (data != null){
            dataList.add(data);
            sb.append("===========================\n")
                    .append("    " + data.name + "\n")
                    .append("===========================\n")
                    .append(data.sql+ "\n\n\n");
        }
    }
    private Data parseSqlTask(Task task) {
        String sql = JSONObject.parseObject(task.params).getString("sql");
        return new Data(task.name, checkSqlIsStartByN(sql));
    }

    private Data parseDataxTask(Task task) {
        String sql = JSONObject.parseObject(task.params).getString("sql");
        return new Data(task.name, checkSqlIsStartByN(sql));
    }
    public void exec() {
        parser();
    }

    private String checkSqlIsStartByN(String sql){
        while (sql.startsWith("\n")){
            sql = sql.replaceFirst("\n","");
        }
        return sql;
    }
    public void close() {
        System.out.println("project process is success");
    }

    public ProjectProcessJsonParser() {
    }

    public ProjectProcessJsonParser(String[] processNameArr) {
        this.processNameArr = processNameArr;
    }

    public static void main(String[] args) {
        new ProjectProcessJsonParser().exec();
    }
}
