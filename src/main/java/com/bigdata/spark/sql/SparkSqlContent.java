package com.bigdata.spark.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSqlContent {
    private String sqlStr;
    /**
     * 参数形式：参数名::参数值
     */
    private Map<String, String> paramMap;
    private String alias;
    private String dbName;
    private List<SparkSqlCondition> condList = new ArrayList<>();

    public SparkSqlContent(String sql, Map<String, String> paramMap, String alias, String dbName) {
        this.sqlStr = sql;
        this.paramMap = paramMap;
        this.alias = alias;
        if(dbName.equalsIgnoreCase("owt")) {
            this.dbName = "prod.owt";
        }
        if(dbName.equalsIgnoreCase("mall") || dbName.equalsIgnoreCase("biz")) {
            this.dbName = "prod.mall";
        }
    }

    public List<SparkSqlCondition> getCondList() {
        return condList;
    }

    public void setCondList(List<SparkSqlCondition> condList) {
        this.condList = condList;
    }

    public String getSqlStr() {
        genExecSparkSql();
        System.out.println("==================== 执行sql：" + sqlStr);
        return sqlStr;
    }

    public Map<String, String> getParamMap() {
        return paramMap;
    }

    public String getAlias() {
        return alias;
    }

    public String getDbName() {
        return dbName;
    }

    public void setParamMap(Map<String, String> paramMap) {
        this.paramMap = paramMap;
    }

    private String genExecSparkSql() {
        if(null == paramMap || 0 == paramMap.size()) {
            return sqlStr;
        }
        for(Map.Entry<String, String> entry : paramMap.entrySet()) {
            sqlStr = sqlStr.replace("{" + entry.getKey()+ "}", entry.getValue());
        }
        return sqlStr;
    }

    public void appendSqlContent(String content) {
        sqlStr += content;
    }

    public void parseSqlComplete() {
        genExecSparkSql();
    }

    public static void main(String[] args) {

        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("t1", "1111111");
        paramMap.put("t2", "2222222");
        SparkSqlContent sparkSql = new SparkSqlContent("sjdfois osij{t1}fosidjfo soid{t2}jfosif", paramMap, "t1", "owt");
        System.out.println(sparkSql.getSqlStr());
    }
}
