package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class PurchaseOrderRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("purchase_order");
        sql.append("( id INT ,tenant_id INT,out_id STRING,biz_code STRING,buyer_id INT,buyer_name STRING,pay_status STRING," +
                "pay_at TIMESTAMP,pay_expire_at TIMESTAMP ,status STRING,enable_status INT ,paid_amount INT,sku_origin_total_amount INT," +
                "sku_adjust_amount INT,sku_discount_total_amount INT,ship_fee_origin_amount INT,ship_fee_adjust_amount INT," +
                "ship_fee_discount_total_amount INT,tax_fee_origin_amount INT,tax_fee_adjust_amount INT,tax_fee_discount_total_amount INT," +
                "tag INT,device_source STRING,payment_json STRING,buyer_phone STRING,memberIntegralTradeNo STRING ,version INT," +
                "updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE purchase_order ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE purchase_order ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_purchase_order ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.purchase_order select po.* from ali_prod_mysql.purchase_order po " +
                "left join ali_prod_mysql.tmp_purchase_order po1 on po1.id = po.id where po1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.purchase_order select * from ali_prod_mysql.tmp_purchase_order";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_purchase_order select * from ali_prod_mysql.tmp_purchase_order where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.purchase_order where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_purchase_order like ali_prod_mysql.purchase_order ";
        hiveJdbcTemplate.execute(sql);
    }
}
