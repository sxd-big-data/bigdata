package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class OrdersRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("`order`");
        sql.append("( id INT,tenant_id INT, out_id STRING,purchase_order_id INT,device_source STRING,biz_code STRING,buyer_id INT" +
                " ,buyer_name STRING ,shop_name STRING,shop_id INT,enable_status INT,pay_status STRING,delivery_status STRING," +
                "receive_status STRING,reverse_status STRING,pay_at TIMESTAMP,shipping_at TIMESTAMP,confirm_at TIMESTAMP," +
                "paid_amount INT ,sku_origin_total_amount INT,sku_adjust_amount INT,sku_discount_total_amount INT ,ship_fee_origin_amount INT," +
                "ship_fee_adjust_amount INT,ship_fee_discount_total_amount INT,tax_fee_origin_amount INT,tax_fee_adjust_amount INT," +
                "tax_fee_discount_total_amount INT,discount_detail STRING,delivery_address_city STRING,delivery_address_cityId STRING," +
                "delivery_address_detail STRING,delivery_address_extra STRING,delivery_address_fullAddressDetail STRING," +
                "delivery_address_phone STRING,delivery_address_province STRING,delivery_address_provinceId STRING,delivery_address_receiveUserName TIMESTAMP," +
                "delivery_address_region STRING,delivery_address_regionId STRING,delivery_address_street STRING,delivery_address_streetId STRING," +
                "delivery_address_userId STRING,buyer_notes STRING,shop_notes STRING,tag INT,store_id STRING,store_name STRING," +
                "store_type STRING,settleAccountId STRING,settleAccountName STRING,sellerId STRING,sellerName STRING,relatedOrderId STRING," +
                "deliverId STRING,deliverName STRING,deliverPhone STRING,syncSignal STRING,outOrderId STRING,buyer_phone STRING," +
                "needUpstairs STRING,needMilkBox STRING,milkSiteId STRING,milkSiteName STRING,regionId STRING,regionName STRING,operatorType STRING," +
                "paymentMethod STRING,channelCode STRING,channelName STRING,invoiceStatus STRING,operatorName STRING,isSupportVATInvoice STRING," +
                "shipment_start_time STRING,selfDelivery STRING,sku_weight STRING,operatorId STRING,appointment_time STRING,pick_confirm_code STRING," +
                "is_in_group STRING,cartLineIds STRING,paymentOperatorId STRING,paymentOperatorName STRING,deviceSource STRING," +
                "channel_id STRING,operatorMobile STRING,erpAccountCode STRING,version INT,updated_by STRING,created_at TIMESTAMP," +
                "updated_at TIMESTAMP,accomplish_at TIMESTAMP,delivery_address_old_city STRING,delivery_address_old_cityId STRING," +
                "delivery_address_old_detail STRING,delivery_address_old_extra STRING,delivery_address_old_fullAddressDetail STRING," +
                "delivery_address_old_phone STRING,delivery_address_old_province STRING,delivery_address_old_provinceId STRING," +
                "delivery_address_old_receiveUserName STRING,delivery_address_old_region STRING,delivery_address_old_regionId STRING," +
                "delivery_address_old_street STRING,delivery_address_old_streetId STRING,delivery_address_old_userId STRING," +
                "delivery_address_old_city2 STRING,delivery_address_old_cityId2 STRING,delivery_address_old_detail2 STRING," +
                "delivery_address_old_extra2 STRING,delivery_address_old_fullAddressDetail2 STRING,delivery_address_old_phone2 STRING," +
                "delivery_address_old_province2 STRING,delivery_address_old_provinceId2 STRING,delivery_address_old_receiveUserName2 STRING," +
                "delivery_address_old_region2 STRING,delivery_address_old_regionId2 STRING,delivery_address_old_street2 STRING," +
                "delivery_address_old_streetId2 STRING,delivery_address_old_userId2 STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE `order` ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE `order` ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_order ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.`order` select o.* from ali_prod_mysql.`order` o " +
                "left join ali_prod_mysql.tmp_order o1 on o1.id = po.id where o1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.order select * from ali_prod_mysql.tmp_order";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_order select * from ali_prod_mysql.tmp_order where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.`order` where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table IF NOT EXISTS create table  ali_prod_mysql.tmp_order like ali_prod_mysql.`order` ";
        hiveJdbcTemplate.execute(sql);
    }
}
