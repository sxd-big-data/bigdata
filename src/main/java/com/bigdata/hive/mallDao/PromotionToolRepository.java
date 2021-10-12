package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class PromotionToolRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("promotion_tool");
        sql.append("(id BIGINT, tenant_id BIGINT,name STRING,`desc` STRING,`group` INT,code STRING,`level` INT,`scope` STRING," +
                "feature STRING,condition_json STRING,action_json STRING,qualification_json STRING,exclude_with_json STRING," +
                "include_with_json STRING,status INT,label STRING,storeType STRING,deviceSource STRING,updated_by STRING," +
                "created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE promotion_tool ";
        hiveJdbcTemplate.execute(sql);
    }
}
