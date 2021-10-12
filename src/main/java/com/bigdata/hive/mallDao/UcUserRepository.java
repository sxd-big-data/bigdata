package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class UcUserRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("uc_user");
        sql.append("( id BIGINT , pk BIGINT ,tenant_id INT,username STRING,nickname STRING,avatar STRING,mobile STRING," +
                "email STRING,password STRING, pwd_expire_at TIMESTAMP,enabled BIGINT,locked BIGINT,deleted BIGINT," +
                "channel STRING,channel_type STRING,source STRING,source_type STRING,tag STRING,version INT,buyerId STRING," +
                "assistantVersion STRING,unionId STRING,gender STRING,yzOpenId STRING,updated_by STRING," +
                "last_login_at TIMESTAMP,created_at TIMESTAMP,updated_at TIMESTAMP,record_create_msg BIGINT,record_update_msg BIGINT )");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE uc_user ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE uc_user ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_uc_user ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.uc_user select u.* from ali_prod_mysql.uc_user u left join " +
                "ali_prod_mysql.tmp_uc_user u1 on u1.id = u.id where u1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.uc_user select * from ali_prod_mysql.tmp_uc_user";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_uc_user select * from ali_prod_mysql.tmp_uc_user where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.uc_user where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_uc_user like ali_prod_mysql.uc_user ";
        hiveJdbcTemplate.execute(sql);
    }
}
