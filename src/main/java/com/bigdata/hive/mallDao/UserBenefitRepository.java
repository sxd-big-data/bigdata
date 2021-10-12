package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class UserBenefitRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("user_benefit");
        sql.append("( id INT ,tenant_id BIGINT,user_id INT,shop_id INT,owner INT,activity_id INT,code STRING,stage INT," +
                "`type` STRING,resource_id STRING,source STRING,quota_meta INT,budget_id INT,total INT,locked INT,used INT," +
                "start_at TIMESTAMP,expired_at TIMESTAMP,status INT,couponType STRING,remark STRING,couponResource STRING," +
                "updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE user_benefit ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE user_benefit ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_user_benefit ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.user_benefit select ub.* from ali_prod_mysql.user_benefit ub " +
                "left join ali_prod_mysql.tmp_user_benefit ub1 on ub1.id = ub.id where ub1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.user_benefit select * from ali_prod_mysql.tmp_user_benefit";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_user_benefit select * from ali_prod_mysql.tmp_user_benefit where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.user_benefit where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_user_benefit like ali_prod_mysql.user_benefit ";
        hiveJdbcTemplate.execute(sql);
    }
}
