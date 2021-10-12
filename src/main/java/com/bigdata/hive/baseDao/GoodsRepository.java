package com.bigdata.hive.baseDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class GoodsRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("goods");
        sql.append("(id INT, goods_code STRING,erp_code STRING,goods_type INT,goods_name STRING,goods_abbreviation STRING," +
                "`type` STRING,boxes_relation STRING,tax_type_code STRING,tax_rate BIGINT,status INT,producer STRING,repertory STRING," +
                "sell STRING,brand STRING,expiration INT,weight STRING,suttle STRING,rougt_weight STRING,supplier STRING," +
                "place_origin STRING,enquiry_ledd_time INT,remark STRING,created_at TIMESTAMP,updated_at TIMESTAMP,create_by STRING," +
                "update_by STRING,flag INT,supplierType INT,barCode STRING,yto INT,producerName STRING,producerId INT,xxwSupplierCode STRING," +
                "xxwSupplierType INT,goodsClassify STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE goods ";
        hiveJdbcTemplate.execute(sql);
    }
}
