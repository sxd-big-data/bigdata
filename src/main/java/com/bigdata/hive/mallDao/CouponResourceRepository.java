package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class CouponResourceRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("coupon_resource");
        sql.append("( id INT , tenant_id BIGINT,user_id INT,shop_id INT,activity_id INT,activity_name STRING,user_coupon_id INT," +
                "code STRING,value STRING,status INT,issued_at TIMESTAMP,redeemed_at TIMESTAMP,used_at TIMESTAMP,certified_at TIMESTAMP," +
                "invalid_at TIMESTAMP,start_at TIMESTAMP,expired_at TIMESTAMP,extra_json STRING,created_at TIMESTAMP,updated_at TIMESTAMP,assign_session  STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE coupon_resource ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE coupon_resource ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_coupon_resource ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.coupon_resource select cr.* from ali_prod_mysql.coupon_resource cr left join" +
                " ali_prod_mysql.tmp_coupon_resource cr1 on cr1.id = cr.id where cr1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.coupon_resource select * from ali_prod_mysql.tmp_coupon_resource";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_coupon_resource select * from ali_prod_mysql.tmp_coupon_resource where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.coupon_resource where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_coupon_resource like ali_prod_mysql.coupon_resource ";
        hiveJdbcTemplate.execute(sql);
    }
}
