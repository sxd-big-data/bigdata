package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class UcUserThirdAccountRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("uc_user_third_account");
        sql.append("( id INT , user_id INT,account_id STRING,account_name STRING,account_type STRING,app_id STRING,open_id STRING," +
                "session_key STRING,openid STRING,unionid STRING,country STRING,gender STRING,province STRING,avatarUrl STRING," +
                "city STRING,nickName STRING,`language` STRING,sex STRING,headimgurl STRING,privilege STRING," +
                "deleted INT,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE uc_user_third_account ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE uc_user_third_account ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_uc_user_third_account ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.uc_user_third_account select uut.* from ali_prod_mysql.uc_user_third_account uut " +
                "left join ali_prod_mysql.tmp_uc_user_third_account uut1 on uut1.id = uut.id where uut1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.uc_user_third_account select * from ali_prod_mysql.tmp_uc_user_third_account";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_uc_user_third_account select * from ali_prod_mysql.tmp_uc_user_third_account where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.uc_user_third_account where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_uc_user_third_account like ali_prod_mysql.uc_user_third_account ";
        hiveJdbcTemplate.execute(sql);
    }
}
