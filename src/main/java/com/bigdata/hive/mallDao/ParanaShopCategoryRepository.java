package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class ParanaShopCategoryRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("parana_shop_category");
        sql.append("(id INT, shop_id INT,tenant_id INT,name STRING,pid INT,logo STRING,`level` INT,`path` STRING,has_children INT," +
                "`type` INT,`index` INT,disclosed INT,status INT,has_bind INT,tag STRING,recommendItemId STRING,created_at TIMESTAMP,updated_at TIMESTAMP,updated_by STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE parana_shop_category ";
        hiveJdbcTemplate.execute(sql);
    }
}
