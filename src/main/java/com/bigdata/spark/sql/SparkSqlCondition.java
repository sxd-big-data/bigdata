package com.bigdata.spark.sql;

import java.util.HashMap;
import java.util.Map;

/**
 * 暂时仅支持ignoreempty条件判断
 */
public class SparkSqlCondition {
    public static String COND_IGNORE_NULL_START = "<ignoreEmpty>";
    public static String COND_IGNORE_NULL_END = "</ignoreEmpty>";
    private String condType;
    private String condStr;

    public String getCondType() {
        return condType;
    }

    public void setCondType(String condType) {
        this.condType = condType;
    }

    public String getCondStr() {
        return condStr;
    }

    public void setCondStr(String condStr) {
        this.condStr = condStr;
    }

    public String genCondSql(Map<String, String> paramMap) {
        String sqlStr = " ";
        if(COND_IGNORE_NULL_START.equals(condType)) {
            // 抽取conditon中参数名称
            String paramName = condStr.substring(condStr.indexOf("{") + 1, condStr.indexOf("}"));
            if(null == paramMap.get(paramName) || "".equals(paramMap.get(paramName))) {
                return sqlStr;
            } else {
                return condStr.replace("{" + paramName + "}", paramMap.get(paramName));
            }
        }
        return sqlStr;
    }

    public static void main(String[] args) {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("bizType", "1111");
        String condContent = "and c.business_type in ({bizType})  ";
        String paramName = condContent.substring(condContent.indexOf("{") + 1, condContent.indexOf("}"));
        System.out.println(paramName);
        if(null == paramMap.get(paramName) || "".equals(paramMap.get(paramName))) {
            System.out.println("==============");
        } else {
            System.out.println(condContent.replace("{" + paramName + "}", paramMap.get(paramName)));
        }

        String val = "      <ignoreEmpty>  and c.business_type in ({bizType})  </ignoreEmpty>";
        SparkSqlCondition cond = new SparkSqlCondition();
        cond.setCondType(SparkSqlCondition.COND_IGNORE_NULL_START);
        cond.setCondStr(val.substring(
                val.indexOf(SparkSqlCondition.COND_IGNORE_NULL_START) + SparkSqlCondition.COND_IGNORE_NULL_START.length()
                , val.indexOf(SparkSqlCondition.COND_IGNORE_NULL_END)));

        System.out.println("----------------" + cond.genCondSql(paramMap));
    }
}
