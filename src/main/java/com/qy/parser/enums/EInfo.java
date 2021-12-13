package com.qy.parser.enums;

import com.qy.parser.bean.EPathData;
import com.qy.parser.utils.FileUtils;
import com.qy.parser.utils.PropertiesUtils;
import static com.qy.parser.enums.EPropertiesInfo.*;

public class EInfo {
    /**
     * base enum
     */

    public static final String BASE_PATH = "C:\\Users\\86000014\\Desktop\\bracelet\\src\\main\\resources";
    public static final String BACKSLASH = "\\";
    public static final String BASE_PROPERTIES_PATH = BASE_PATH + BACKSLASH + "pj";
    public static final String BASE_PROPERTIES_NAME = "common.properties";
    public static final String BASE_PROPERTIES_FILE_PATH = BASE_PROPERTIES_PATH + BACKSLASH + BASE_PROPERTIES_NAME;

    public static final String SHUCANG_NAME = PropertiesUtils.getValueNotNull(PROJECT_NAME);
    public static final String USER = PropertiesUtils.getValueNotNull(PROJECT_USER);

    public static final String SHUCANG_PATH = BASE_PATH + BACKSLASH + USER + BACKSLASH + SHUCANG_NAME;
    public static final String DB = "db";
    public static final String UNDERLINE = "_";

    public static final String CLIFE = "clife";
    public static final String ODS = "ods";
    public static final String DWD = "dwd";
    public static final String DWS = "dws";
    public static final String ADS = "ads";
    public static final String DIM = "dim";
    public static final String PARSER = "parser";
    public static final String PROJECT_PROCESS_JSON = "projectprocessjson";
    public static final String PROJECT_PROCESS_JSON_FILE_NAME = PROJECT_PROCESS_READ_PROCESSJSON_FILENAME;
    public static final String TRANSFORM = "transform";
    public static final String MYSQL = "mysql";
    public static final String HIVE = "hive";
    public static final String READ = "read";
    public static final String WRITE = "write";
    public static final String TABLE_FILE = "table_file";
    public static final String TABLE_FILE_NAME = PropertiesUtils.getValueNotNull(PROJECT_TRANSFORM_READ_TABLEFILES_NAME);

    public static final String COMMON_ODS_DATABASE = DB + UNDERLINE + ODS + UNDERLINE + SHUCANG_NAME; // "db_ods_nstc";
    public static final String COMMON_PROCESS_NAME_FILE = SHUCANG_PATH + "\\parser\\common\\process_name.txt";
    public static final String COMMON_HIVE_ODS_TABLE_PREFIX = CLIFE + UNDERLINE + SHUCANG_NAME + UNDERLINE + ODS + UNDERLINE;// "clife_nstc_ods_";
    /**
     * process json
     */

    /**
     * process json 读写文件路径
     */
    public static String READ_PROCESS_JSON_FILE = SHUCANG_PATH + BACKSLASH + PARSER + BACKSLASH + PROJECT_PROCESS_JSON + BACKSLASH +READ + BACKSLASH + PropertiesUtils.getValueNotNull(PROJECT_PROCESS_JSON_FILE_NAME);
    private static String WRITE_PROCESS_JSON_FILE_SUFFIX = SHUCANG_PATH + BACKSLASH + PARSER + BACKSLASH + PROJECT_PROCESS_JSON + BACKSLASH + WRITE;
    public static EPathData PROCESS_JSON_EDATA = new EPathData(READ_PROCESS_JSON_FILE, WRITE_PROCESS_JSON_FILE_SUFFIX);
    /**
     * 数仓需要解析的工作流名称
     */
    public static String[] PROCESS_NAME_ARR = PropertiesUtils.getValueNotNull(PROJECT_PROCESS_NAMES).split(",");


    /**
     * 文件换行符
     */
    public static final String FILE_SEP = "\n";

    /**
     * process json key
     */

    public static final String JSON_DATA = "data";
    public static final String JSON_TOTAL_LIST = "totalList";
    public static final String JSON_TASKS = "tasks";
    public static final String JSON_SQL = "sql";
    /**
     * process json task type
     */
    public static final String DATAX_TYPE = "datax";
    public static final String SQL_TYPE = "sql";
    /**
     * sql文件后缀
     */
    public static final String FILE_SUFFIX_SQL = ".sql";
    public static final Boolean IS_WRITE_PROCESS_JSON = true;

    public static Integer MAX_LEN = 100;
    public static Integer MIN_LEN = 10;

    /**
     * transform
     */
    public static final String TRANSFORM_MYSQL_JDBC = PropertiesUtils.getValueNotNull(PROJECT_TRANSFORM_MYSQL_URL);
    public static final String TRANSFORM_MYSQL_USERNAME = PropertiesUtils.getValueNotNull(PROJECT_TRANSFORM_MYSQL_USERNAME);
    public static final String TRANSFORM_MYSQL_PASSWORD = PropertiesUtils.getValueNotNull(PROJECT_TRANSFORM_MYSQL_PASSWORD);
    public static final String TRANSFORM_PATH_TABLES = SHUCANG_PATH + "transform\\mysql\\table_file\\tables.txt";
    public static final String TRANSFORM_PATH_MYSQL_CREATE_SQL = SHUCANG_PATH + "\\parser\\transform\\mysql\\create_sql\\mysql_create_sql.sql";
    public static final String TRANSFORM_PATH_MYSQL_SELECT_SQL = SHUCANG_PATH + "\\parser\\transform\\mysql\\select_sql\\mysql_select_sql.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_ODS_SQL = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_ods.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DW_SQL = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_dw.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_ODS_PATH = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_ods.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DWD_PATH = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_dwd.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DWS_PATH = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_dws.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_ADS_PATH = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_ads.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DIM_PATH = SHUCANG_PATH + "\\parser\\transform\\hive\\create_sql\\hive_create_sql_dim.sql";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DW_FIELD_CSV = SHUCANG_PATH + "\\parser\\transform\\hive\\create_csv\\field\\Yearning_Data.csv";
    public static final String TRANSFORM_PATH_HIVE_CREATE_DW_PARTITION_CSV = SHUCANG_PATH + "\\parser\\transform\\hive\\create_csv\\partition\\Yearning_Data.csv";
    public static final String TRANSFORM_PATH_HIVE_SELECT_SQL = SHUCANG_PATH + "\\parser\\transform\\hive\\select_sql\\hive_select_sql.sql";
    public static final String TRANSFORM_PATH_NO_TIME_TABLES = SHUCANG_PATH + "\\parser\\transform\\mysql\\table_file\\no_time_table.txt";
    public static final Boolean IS_WRITE_TRANSFORM_JSON = true;

    /**
     * keywords
     */
    public static final String KEYWORDS_PATH_MYSQL = SHUCANG_PATH + "\\parser\\keyword\\mysql\\mysql_keyword.txt";
    public static final String KEYWORDS_PATH_HIVE = SHUCANG_PATH + "\\parser\\keyword\\hive\\hive_keyword.txt";






}
