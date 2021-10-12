package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class OrderLinesRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("order_line");
        sql.append("( id INT,tenant_id INT,out_id STRING,biz_code STRING,purchase_order_id INT,order_id INT,buyer_id INT," +
                "buyer_name STRING,shop_id INT,shop_name STRING,warehouse_code_plan STRING,warehouse_code_actual STRING," +
                "pay_status STRING,delivery_status STRING,receive_status STRING,reverse_status STRING,enable_status INT," +
                "refund_at TIMESTAMP,confirm_at TIMESTAMP,sku_id INT,sku_code STRING,sku_name STRING,sku_image STRING," +
                "sku_attr STRING,quantity INT,paid_amount INT,sku_origin_total_amount INT,sku_adjust_amount INT,sku_discount_total_amount INT," +
                "ship_fee_origin_amount INT,ship_fee_adjust_amount INT,ship_fee_discount_total_amount INT,tax_fee_origin_amount INT," +
                "tax_fee_adjust_amount INT,tax_fee_discount_total_amount INT,discount_detail STRING,device_source STRING," +
                "tag INT,master_id INT,outOrderLineId STRING,unitPrice STRING,fullUnit STRING,goodCode STRING,goodName STRING," +
                "calendarTemplateId STRING,calendarTemplateName STRING,deliveryQuantity STRING,deliveryTimeType STRING," +
                "calendarStartTime STRING,calendarEndTime STRING,calendarStartInterval STRING,calendarRenderTime STRING," +
                "completeQuantity STRING,deliveryQuantityMax STRING,itemType STRING,originalPrice STRING,item_type STRING," +
                "uniqueKey STRING,businessName STRING,businessCode STRING,cartLineId STRING,deliveryQuantityMin STRING," +
                "categoryList STRING,sku_weight STRING,shopType STRING,businessType STRING,calendarPeriod STRING,purchaseQuantityMax STRING," +
                "deliverySumQuantity STRING,deliverMethod STRING,version INT,updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP," +
                "sku_extra STRING,item_id INT,item_name STRING,sku_line_id INT,bundle_id STRING,store_id INT,store_name STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE order_line ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE order_line ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_order_line ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.order_line select ol.* from ali_prod_mysql.order_line ol left join " +
                "ali_prod_mysql.tmp_order_line ol1 on ol1.id = ol.id where ol1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.order_line select * from ali_prod_mysql.tmp_order_line";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_order_line select * from ali_prod_mysql.tmp_order_line where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.order_line where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_order_line like ali_prod_mysql.order_line ";
        hiveJdbcTemplate.execute(sql);
    }
}
