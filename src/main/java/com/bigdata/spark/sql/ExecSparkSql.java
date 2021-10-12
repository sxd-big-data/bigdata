package com.bigdata.spark.sql;

import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.util.List;

/**
 * 当前仅能按照prepareSql顺序执行，最后执行execsql生成结果
 *
 */
public class ExecSparkSql {
    private List<SparkSqlContent> prepareSql;
    private SparkSqlContent execSql;

    public ExecSparkSql(List<SparkSqlContent> prepare, SparkSqlContent execSql) {
        this.prepareSql = prepare;
        this.execSql = execSql;
        if(null != prepare) {
            for(SparkSqlContent content : prepare) {
                content.parseSqlComplete();
                for(SparkSqlCondition cond : content.getCondList()) {
                    content.appendSqlContent(cond.genCondSql(content.getParamMap()) + " ");
                }
            }
        }
        for(SparkSqlCondition cond : execSql.getCondList()) {
            execSql.appendSqlContent(cond.genCondSql(execSql.getParamMap()) + " ");
        }
        execSql.parseSqlComplete();
    }

    public List<SparkSqlContent> getPrepareSql() {
        return prepareSql;
    }

    public SparkSqlContent getExecSql() {
        return execSql;
    }
}
