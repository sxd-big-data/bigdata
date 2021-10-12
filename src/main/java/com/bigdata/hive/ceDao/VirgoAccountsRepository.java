package com.bigdata.hive.ceDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
@Service
public class VirgoAccountsRepository {
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("virgo_accounts");
        sql.append("( id INT, tenant_id INT,gl_code STRING,owner_id STRING,amount INT,intransit_amount INT,frozen_amount INT," +
                "withhold_amount INT,pay_pass STRING,status STRING,amount_change_at TIMESTAMP,ext1 STRING,ext2 STRING,ext3 STRING," +
                "extra_json STRING,created_at TIMESTAMP,updated_at TIMESTAMP)");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
    }
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE virgo_accounts ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE virgo_accounts ";
        hiveJdbcTemplate.execute(sql);
    }

    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_virgo_accounts ";
        hiveJdbcTemplate.execute(sql);
    }

    public String[] updateHisData() {
        //先把原表中需要更新的数据删除
        String sql ="insert overwrite table ali_prod_mysql.virgo_accounts select va.* from ali_prod_mysql.virgo_accounts va " +
                "left join ali_prod_mysql.tmp_virgo_accounts va1 on va1.id = va.id where va1.id is null; ";
        //将临时表数据追加到原表的尾部。
        sql += "insert into ali_prod_mysql.virgo_accounts select * from ali_prod_mysql.tmp_virgo_accounts";
        return sql.split(";");
    }

    public void deleteAll(){
        String sql = "insert overwrite table ali_prod_mysql.tmp_virgo_accounts select * from ali_prod_mysql.tmp_virgo_accounts where 1=0";
        hiveJdbcTemplate.execute(sql);
    }

    public String queryMaxIdAndCreatedTime(){
        return "select max(id),max(created_at),max(updated_at) from ali_prod_mysql.virgo_accounts where id is not null and created_at is not null and updated_at is not null;";
    }

    public void createTmpTable(){
        String sql = "create table  ali_prod_mysql.tmp_virgo_accounts like ali_prod_mysql.virgo_accounts ";
        hiveJdbcTemplate.execute(sql);
    }
}
