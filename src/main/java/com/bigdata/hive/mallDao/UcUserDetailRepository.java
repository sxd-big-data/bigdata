package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class UcUserDetailRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("uc_user_detail");
        sql.append("( userId BIGINT ,isBirthday STRING,gender STRING,realName STRING,idNumber STRING,maritalStatus STRING,incomeLevel STRING," +
                "blockId STRING,relationship STRING,mmAlipay STRING,address STRING,country STRING,province STRING,avatarUrl STRING,city STRING,nickName STRING,`language` STRING," +
                "provinceName STRING,cityName STRING,district STRING,districtName STRING,babyBirthday TIMESTAMP,chooseFor STRING,birthday TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE uc_user_detail ";
        hiveJdbcTemplate.execute(sql);
    }
}
