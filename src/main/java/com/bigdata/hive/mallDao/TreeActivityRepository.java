package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class TreeActivityRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("tree_activity");
        sql.append("(id BIGINT,tenant_id BIGINT,name STRING,display_name STRING,`desc` STRING,shop_id INT,tool_id INT,tool_key STRING," +
                "feature STRING,`group` INT,code STRING,sub_code STRING,campaign_id INT,outer_id STRING,`level` INT,label STRING," +
                "status INT,sub_status INT,tree_json STRING,qualification_json STRING,start_at TIMESTAMP,expired_at TIMESTAMP," +
                "quota_metas INT,exclude_with_json STRING,include_with_json STRING,warm_start_at TIMESTAMP,warm_end_at TIMESTAMP," +
                "stores STRING,entryMode STRING,couponType STRING,storeScope STRING,frontObtain STRING,deviceSource STRING," +
                "frontDisplay STRING,relativeDays STRING,updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tree_activity ";
        hiveJdbcTemplate.execute(sql);
    }
}
