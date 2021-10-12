package com.bigdata.hive.hiveDao;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Service;

import com.bigdata.hive.entity.BusReceiverEntity;

@Service
public class BusReceiverHiveRepository{

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * <li>Description: TODO </li>
     */
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
       /* StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("bus_receiver ");
        sql.append("(id BIGINT comment '主键ID' " +
                ",name STRING  comment '姓名' " +
                ",address STRING comment '地址'" +
                ",en_name STRING comment '拼音名字'" +
                ",member_family INT comment '家庭成员'" +
                ",createDate DATE comment '创建时') ");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
       // hiveJdbcTemplate.execute(sql.toString());*/
    }

    /**
     * <li>Description: TODO </li>
     *
     * @param pathFile TODO
     */
    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE bus_receiver";
        hiveJdbcTemplate.execute(sql);
    }


    /**
     * <li>Description: TODO </li>
     *
     * @param busReceiverEntity 实体
     */
    public void insert(BusReceiverEntity busReceiverEntity) {
    	hiveJdbcTemplate.execute("set hive.exec.mode.local.auto=true");
        hiveJdbcTemplate.update("insert into bus_receiver(id,name,address,en_name,member_family) values(?,?,?,?,?)",
                new PreparedStatementSetter(){
                    @Override
                    public void setValues(PreparedStatement ps) throws SQLException {
                        ps.setLong(1, busReceiverEntity.getId());
                        ps.setString(2,busReceiverEntity.getName());
                        ps.setString(3,busReceiverEntity.getAddress());
                        ps.setString(4,busReceiverEntity.getEnName());
                        ps.setInt(5,busReceiverEntity.getMemberFamily());
                    }
                }
        );
    }

    public void deleteAll(){
        String sql = "insert overwrite table bus_receiver select * from bus_receiver where 1=0";
        hiveJdbcTemplate.execute(sql);
    }
}
