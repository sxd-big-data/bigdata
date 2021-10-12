package com.bigdata.hive.mallDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class PayJournalRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("pay_journal");
        sql.append("( id INT ,tenant_id INT,pay_channel STRING,buyer_id INT,payment_order_id INT,external_serial_no STRING," +
                "journal_status STRING,amount INT ,channel_discount_amount INT,pay_account STRING,refund_status STRING,related_journal_id INT," +
                "finished_at TIMESTAMP,operator STRING,deliverId STRING,deviceNo STRING,orderPaymentInfoId STRING,cardNo STRING,merReference STRING,milkTicketNo STRING," +
                "operatorId STRING,operatorName STRING,milkTicketAmount STRING,ticket STRING,buyerId STRING,`date` STRING,sysReferenceNo STRING,supple STRING," +
                "`time` STRING,version INT,updated_by STRING,created_at TIMESTAMP,updated_at TIMESTAMP,order_id STRING,old_channel STRING)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }

    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE pay_journal ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE pay_journal ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_pay_journal ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.pay_journal select pj.* from ali_prod_mysql.pay_journal pj " +
                "left join ali_prod_mysql.tmp_pay_journal pj1 on pj1.id = pj.id where pj1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.pay_journal select * from ali_prod_mysql.tmp_pay_journal";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_pay_journal select * from ali_prod_mysql.tmp_pay_journal where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.pay_journal where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_pay_journal like ali_prod_mysql.pay_journal ";
        hiveJdbcTemplate.execute(sql);
    }
}
