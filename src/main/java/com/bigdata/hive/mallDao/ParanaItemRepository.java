package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class ParanaItemRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("parana_item");
        sql.append("( id INT,extension_type INT,spu_id INT,unit STRING,tenant_id INT,category_id INT,item_code STRING,pos_code STRING," +
                "shop_id INT,shop_name STRING,brand_id INT,brand_name STRING,sale_unit_name STRING,inventory_unit_name STRING," +
                "delivery_fee_temp_id INT,name STRING,advertise STRING,main_image STRING,video_url STRING,status INT,`type` INT," +
                "business_type INT,`exp` INT,sku_attributes_json STRING,other_attributes_json STRING,categoryList STRING," +
                "frontCategoryIdList STRING,erpCode STRING,grossWeight STRING,fullUnit STRING,notice STRING,barCodeList STRING," +
                "description STRING,productionPlace STRING,netWeight STRING,attributesExplain STRING,bit_tag INT,md5_info STRING," +
                "version INT,created_at TIMESTAMP,updated_at TIMESTAMP,updated_by STRING,audit_status INT,outer_id STRING," +
                "activity_id INT,purchase_quantity INT,min_order_quantity INT,supplier STRING,purchase_rule STRING,purchase_mode STRING," +
                "gift INT,effective_start_time STRING,effective_stop_time STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE parana_item ";
        hiveJdbcTemplate.execute(sql);
    }
}
