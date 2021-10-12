package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class SimpleActivityRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("simple_activity");
        sql.append("(id BIGINT, tenant_id BIGINT,name STRING,display_name STRING,shop_id INT,code STRING,feature STRING," +
                "campaign_id INT,tree_id INT,data_provider STRING,`level` INT,status INT,product_key STRING,product_key_type INT," +
                "fee_type INT,discount_type INT,discount_value INT,start_at TIMESTAMP,expired_at TIMESTAMP,quota_metas INT," +
                "exclude_with_json STRING,include_with_json STRING,warm_start_at TIMESTAMP,warm_end_at TIMESTAMP,stores STRING," +
                "entryMode STRING,couponType STRING,online STRING,storeScope STRING,frontObtain STRING,deviceSource STRING," +
                "frontDisplay STRING,relativeDays STRING,updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE simple_activity ";
        hiveJdbcTemplate.execute(sql);
    }
}
