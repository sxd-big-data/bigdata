package com.bigdata.hive.owtDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class PaymentSettlementRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("payment_settlement");
        sql.append("( id INT , tenant_id INT ,channel STRING ,gateway_commission INT,gateway_rate INT,trade_fee BIGINT," +
                "actual_income_fee BIGINT,trade_type STRING,trade_no STRING,gateway_trade_no STRING,pay_status STRING," +
                "trade_finished_at TIMESTAMP,check_status BIGINT,error_type STRING,extra_json STRING ,created_at TIMESTAMP," +
                "updated_at TIMESTAMP,biz_id STRING,mer_id STRING,version BIGINT,mch_id BIGINT,mch_name STRING,settle_at STRING" +
                ") ");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public  void loadData(String filePath){
        String sql = "load data inpath '"+filePath+"' into table payment_settlement ";
        hiveJdbcTemplate.execute(sql);
    }

    public void createTmpTable(){
        String sql = "create table  tmp_payment_settlement like payment_settlement ";
        hiveJdbcTemplate.execute(sql);
    }

    public void updateHisData(){
        //删除原表中需要更新的数据
        String sql = "insert overwrite payment_settlement select c.* from payment_settlement c left join " +
                "tmp_payment_settlement c1 on c1.id_spt = c.id_spt where  c1.id_spt is null";
        hiveJdbcTemplate.execute(sql);
        // 将临时表数据增加到原表数据中
        sql = "insert into TMP_IMG_MAINT_ASSOC select * from payment_settlement";
        hiveJdbcTemplate.execute(sql);
    }

    public void deleteAll(){
        String sql = "insert overwrite table payment_settlement select * from payment_settlement where 1=0";
        hiveJdbcTemplate.execute(sql);
    }


    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.payment_settlement  where id is not null and created_at is not null and updated_at is not null;";
    }
}
