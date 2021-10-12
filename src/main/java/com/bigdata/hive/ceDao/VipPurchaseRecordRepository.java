package com.bigdata.hive.ceDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class VipPurchaseRecordRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("vip_purchase_record");
        sql.append("( id INT, laneId INT,laneName STRING,memberId INT,templateId INT,templateName STRING,serviceId INT," +
                "serviceName STRING,itemId INT,orderId INT,duration INT,price INT,reducePrice INT,bonusDays INT,paidAmount INT," +
                "payType STRING,purchaseAt TIMESTAMP,effectiveStartAt TIMESTAMP,status STRING,refundAmount INT,refundReason STRING," +
                "refundOrderId INT,refundAt TIMESTAMP,effectiveEndAt TIMESTAMP,createdAt TIMESTAMP,updatedAt TIMESTAMP,updatedBy STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE vip_purchase_record ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE vip_purchase_record ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_vip_purchase_record ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.vip_purchase_record select vpr.* from ali_prod_mysql.vip_purchase_record vpr " +
                "left join ali_prod_mysql.tmp_vip_purchase_record vpr1 on vpr1.id = vpr.id where vpr1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.vip_purchase_record select * from ali_prod_mysql.tmp_vip_purchase_record";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_vip_purchase_record select * from ali_prod_mysql.tmp_vip_purchase_record where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(createdAt) created_at ,max(updatedAt) updated_at from ali_prod_mysql.vip_purchase_record where id is not null and createdAt is not null and updatedAt is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_vip_purchase_record like ali_prod_mysql.vip_purchase_record ";
        hiveJdbcTemplate.execute(sql);
    }
}
