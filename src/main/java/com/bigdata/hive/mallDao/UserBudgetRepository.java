package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class UserBudgetRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("user_budget");
        sql.append("( id BIGINT, tenant_id INT ,user_id INT,activity_id INT,code STRING,total INT,locked INT,used INT," +
                "`year` INT,`month` INT,status INT,extra_json STRING,version INT,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE user_budget ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE user_budget ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_user_budget ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.user_budget select u.* from ali_prod_mysql.user_budget u " +
                "left join ali_prod_mysql.tmp_user_budget u1 on u1.id = u.id where u1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.user_budget select * from ali_prod_mysql.tmp_user_budget";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_user_budget select * from ali_prod_mysql.tmp_user_budget where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.user_budget where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_user_budget like ali_prod_mysql.user_budget ";
        hiveJdbcTemplate.execute(sql);
    }
}
