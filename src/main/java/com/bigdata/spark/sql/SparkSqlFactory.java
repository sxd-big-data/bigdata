package com.bigdata.spark.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.bigdata.spark.constons.SqlConstEnum;
import com.bigdata.spark.utils.SparkUtil;

public class SparkSqlFactory {

    private static String PARAM_STR_SEPERATOR = ";;";
    private static String PARAM_VALUE_SEPERATOR = "##";
    private static String LINE_LABEL_VALUE_SEPERATOR = "::";
    private static String LINE_LABEL_NAME_SEPERATOR = "@";
    private static String LINE_COMMENT = "#";
    private static String PREPARE_LABEL = "@prepare";
    private static String EXEC_LABEL = "@exec";
    private static String PARAM_LABEL = "param::";
    private static String END_LABEL = ";";

    public static ExecSparkSql createExecSparkSql(SqlConstEnum sqlConstEnum) {
        if(sqlConstEnum.getSqlName().equals(SqlConstEnum.BASE_ORG.getSqlName())
                || sqlConstEnum.getSqlName().equals(SqlConstEnum.ERP_INFO.getSqlName())) {
            List<SparkSqlContent> sqlList = new ArrayList<>();
            sqlList.add(createSqlContent(sqlConstEnum));
            ExecSparkSql sql = new ExecSparkSql(sqlList,
                    new SparkSqlContent("select * from " + sqlConstEnum.getSqlName(), null, "test", sqlConstEnum.getDbName()));
            return sql;
        }
        // 创建履订单查询
        if(sqlConstEnum.getSqlName().equals(SqlConstEnum.FULFILL_ORDER_GOODS.getSqlName())) {
            List<SparkSqlContent> sqlList = new ArrayList<>();
            sqlList.add(createSqlContent(sqlConstEnum, Arrays.asList("2021-02-01", "2021-02-16")));
            sqlList.add(createSqlContent(SqlConstEnum.BASE_ORG));

            SparkSqlContent sum = createSqlContent(SqlConstEnum.FULFILL_GOODS_SUM);
            ExecSparkSql sql = new ExecSparkSql(sqlList, sum);

            return sql;
        }
        return null;
    }

    public static SparkSqlContent createSqlContent(SqlConstEnum sqlConstEnum) {
        SparkSqlContent sqlContent = new SparkSqlContent(sqlConstEnum.getSqlStr(), null, sqlConstEnum.getSqlName(), sqlConstEnum.getDbName());
        return sqlContent;
    }

    @Deprecated
    public static SparkSqlContent createSqlContent(SqlConstEnum sqlConstEnum, List<String> paramList) {
        SparkSqlContent sqlContent = new SparkSqlContent(sqlConstEnum.getSqlStr(), null, sqlConstEnum.getSqlName(), sqlConstEnum.getDbName());
        return sqlContent;
    }

    public static ExecSparkSql buildExecSparkSqlFromFile(String fullPath, SparkSession spark, List<String> params) {

        Dataset ds   = spark.read().textFile(fullPath);
        boolean prepareStart = false;
        boolean execStart = false;
        List<SparkSqlContent> psqlList = new ArrayList<>();
        SparkSqlContent esql = null;
        Map<String, String> paramMap = new HashMap<>();
        if(null != params && params.size() > 0) {
            paramMap = convert2ParamMap(params);
            System.out.println("=========存在传入参数，以传入参数为准，忽略sql文件中的参数");
        }
        int idx = 0;
        String line = "";
        for(Object val : ds.javaRDD().collect()) {
            line = val.toString().trim();
            if(null == line || "".equals(line)) {
                continue;
            }
            if(line.startsWith(LINE_COMMENT)) {
               continue;
            }
            if(line.startsWith(PREPARE_LABEL)) {
                prepareStart = true;
            }
            if(line.startsWith(EXEC_LABEL)) {
                prepareStart = false;
                execStart = true;
            }
            if(line.startsWith(PARAM_LABEL) && paramMap.isEmpty()) {
                paramMap = convert2ParamMap(line.substring(PARAM_LABEL.length()));
                continue;
            }
            if(prepareStart) {
                if(line.contains(LINE_LABEL_VALUE_SEPERATOR)) {
                    psqlList.add(new SparkSqlContent(line.split(LINE_LABEL_VALUE_SEPERATOR)[1] + " ", paramMap
                            , line.split(LINE_LABEL_VALUE_SEPERATOR)[0].split(LINE_LABEL_NAME_SEPERATOR)[0]
                            , line.split(LINE_LABEL_VALUE_SEPERATOR)[0].split(LINE_LABEL_NAME_SEPERATOR)[1]));
                } else {
                    if(null != psqlList && psqlList.size() > 0) {
                        // 处理conditon
                        if(line.contains(SparkSqlCondition.COND_IGNORE_NULL_START)) {
                            SparkSqlCondition cond = new SparkSqlCondition();
                            cond.setCondType(SparkSqlCondition.COND_IGNORE_NULL_START);
                            cond.setCondStr(line.substring(
                                    line.indexOf(SparkSqlCondition.COND_IGNORE_NULL_START) + SparkSqlCondition.COND_IGNORE_NULL_START.length()
                                    , line.indexOf(SparkSqlCondition.COND_IGNORE_NULL_END)));
                            psqlList.get(psqlList.size() - 1).getCondList().add(cond);
                        } else {
                            if (line.endsWith(END_LABEL)) {
                                psqlList.get(psqlList.size() - 1).appendSqlContent(line.substring(0, line.length() - 1) + " ");
                                psqlList.get(psqlList.size() - 1).appendSqlContent(" t" + idx++);
                            } else {
                                psqlList.get(psqlList.size() - 1).appendSqlContent(line + " ");
                            }
                        }
                    }
                }
            }

            if(execStart) {
                if(line.contains(LINE_LABEL_VALUE_SEPERATOR)) {
                    esql = new SparkSqlContent(line.split(LINE_LABEL_VALUE_SEPERATOR)[1] + " ", null
                            , line.split(LINE_LABEL_VALUE_SEPERATOR)[0].split(LINE_LABEL_NAME_SEPERATOR)[0]
                            , "owt");
                } else {
                    if(null != esql) {
                        if(line.endsWith(END_LABEL)) {
                            // sql 结束，去掉末尾分号
                            esql.appendSqlContent(line.substring(0, line.length() - 1) + " ");
                        } else {
                            esql.appendSqlContent(line + " ");
                        }
                    }
                }
            }
        }
        ExecSparkSql sql = new ExecSparkSql(psqlList, esql);
        return sql;
    }

    public static Map<String, String> convert2ParamMap(List<String> paramList) {
        Map<String, String> paramMap = new HashMap<>();
        for(String param : paramList) {
            paramMap.put(param.split(PARAM_VALUE_SEPERATOR)[0], param.split(PARAM_VALUE_SEPERATOR)[1]);
        }

        return paramMap;
    }

    public static Map<String, String> convert2ParamMap(String paramStr) {
        Map<String, String> paramMap = new HashMap<>();
        String[] arrs = paramStr.split(PARAM_STR_SEPERATOR);
        for(String param : arrs) {
            param = param.trim();
            paramMap.put(param.split(PARAM_VALUE_SEPERATOR)[0], param.split(PARAM_VALUE_SEPERATOR)[1]);
        }
        return paramMap;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkUtil.createSparkSession("SparkSqlFactory");
        List<String> params = new ArrayList<>();
        params.add("storeId##12345678");
        params.add("bizType##1,2");
        ExecSparkSql sql = SparkSqlFactory.buildExecSparkSqlFromFile("D:\\sparktest\\test_param.txt", spark, params);
        for(SparkSqlContent content : sql.getPrepareSql()) {
            System.out.println("==========" + content.getSqlStr());
        }
        spark.stop();
    }
}
