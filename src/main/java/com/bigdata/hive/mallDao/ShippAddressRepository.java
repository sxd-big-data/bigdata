package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class ShippAddressRepository {

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("shipping_address");
        sql.append("( id INT,  user_id BIGINT ,receiver_name STRING,receiver_mobile STRING,receiver_telephone STRING," +
                "province_id INT,province_name STRING,city_id INT,city_name STRING,district_id INT,district_name STRING," +
                "street_id INT,street_name STRING,detail_address STRING,postcode STRING,is_default INT," +
                "created_at TIMESTAMP,updated_at TIMESTAMP,is_delete INT,longitude BIGINT,latitude BIGINT,extra STRING," +
                "`floor` INT ,elevator INT ,md5 STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE shipping_address ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE shipping_address ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_shipping_address ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.shipping_address select sa.* from ali_prod_mysql.shipping_address sa left join " +
                "ali_prod_mysql.tmp_shipping_address sa1 on sa1.id = sa.id where sa1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.shipping_address select * from ali_prod_mysql.tmp_shipping_address";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_shipping_address select * from ali_prod_mysql.tmp_shipping_address where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.shipping_address where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_shipping_address like ali_prod_mysql.shipping_address ";
        hiveJdbcTemplate.execute(sql);
    }
}


