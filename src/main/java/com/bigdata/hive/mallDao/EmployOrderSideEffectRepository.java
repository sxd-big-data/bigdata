package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class EmployOrderSideEffectRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("employ_order_side_effect");
        sql.append("( id INT, tenant_id INT ,product_key INT,product_type INT,order_id String,event_id String,total_cost INT," +
                "`type` INT,buyer_id INT,`year` INT,`month` INT,quantity INT,unit_price INT,`reduce` BIGINT,extra_json STRING," +
                "created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE employ_order_side_effect ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE employ_order_side_effect ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_employ_order_side_effect ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.employ_order_side_effect select eos.* from ali_prod_mysql.employ_order_side_effect eos " +
                "left join ali_prod_mysql.tmp_employ_order_side_effect eos1 on eos1.id = eos.id where eos1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.employ_order_side_effect select * from ali_prod_mysql.tmp_employ_order_side_effect";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_employ_order_side_effect select * from ali_prod_mysql.tmp_employ_order_side_effect where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.employ_order_side_effect where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_employ_order_side_effect like ali_prod_mysql.employ_order_side_effect ";
        hiveJdbcTemplate.execute(sql);
    }
}
