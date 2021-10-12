package com.bigdata.hive.owtDao;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Service;

import com.bigdata.mysql.entity.OrderDeliverDetailData;

@Service
public class AclTreeNodeRepository{

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * <li>Description: TODO </li>
     */
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("acl_tree_node ");
        sql.append("(" + 
        		"  id BIGINT, key STRING, tenant_id BIGINT, biz_id STRING, biz_name STRING, biz_type STRING, biz_type_name STRING, dimension_key STRING, full_path STRING,"
        		+ " permission_delegate BOOLEAN, parent_key STRING, is_user_node BOOLEAN, hash STRING, created_at TIMESTAMP, updated_at TIMESTAMP, deleted BOOLEAN"+           
                ") ");
        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符  
        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
        hiveJdbcTemplate.execute(sql.toString());
//        hiveJdbcTemplate.execute("set hive.exec.mode.local.auto=true");
//    	hiveJdbcTemplate.execute("set hive.exec.reducers.bytes.per.reducer=500000000");
//    	hiveJdbcTemplate.execute("set mapred.reduce.tasks=15");
//        row format delimited fields terminated by ' '  
//        lines terminated by '\n'
    }

    /**
     * <li>Description: TODO </li>
     *
     * @param pathFile TODO
     */
    public void loadOverData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE acl_tree_node ";
        hiveJdbcTemplate.execute(sql);
    }


    /**
     * <li>Description: TODO </li>
     *
     * @param busReceiverEntity 实体
     */
    public void insert(OrderDeliverDetailData busReceiverEntity) {
    	hiveJdbcTemplate.execute("set hive.exec.mode.local.auto=true");
    	hiveJdbcTemplate.execute("set mapreduce.map.memory.mb=3288");
//    	hiveJdbcTemplate.execute("set hive.exec.reducers.bytes.per.reducer=500000000");
//    	hiveJdbcTemplate.execute("set mapred.reduce.tasks=15");
//    	hiveJdbcTemplate.execute("set mapreduce.job.reduces=15");
//    	hiveJdbcTemplate.execute("set hive.execution.engine=spark");
    	
    	
    	
    	
    	
    	
    	
    	
    	hiveJdbcTemplate.update("insert into acl_tree_node(id" + 
        		", code" + 
        		", name" + 
        		", abbreviation" + 
        		", type" + 
        		", status" + 
        		", desc_ription" + 
        		", province_code" + 
        		", province_name" + 
        		", city_code" + 
        		", city_name" + 
        		", region_code" + 
        		", region_name" + 
        		", street_code" + 
        		", street_name" + 
        		", address_detail" + 
        		", full_address" + 
        		", phone" + 
        		", poi_code" + 
        		", operationPositionListJson" + 
        		", province_id" + 
        		", city_id" + 
        		", region_id" + 
        		", street_id" + 
        		", merchant_id" + 
        		", merchant_name" + 
        		", created_at" + 
        		", updated_at" + 
        		", update_by" + 
        		", poiOutId" + 
        		", longitude" + 
        		", latitude" + 
        		", create_by" + 
        		", block_id" + 
        		", organization_key" + 
        		", city" + 
        		", region" + 
        		", shop_id)"
        		+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,)",
               new PreparedStatementSetter(){
                    @Override
                    public void setValues(PreparedStatement ps) throws SQLException {
                    	
                        ps.setLong(1, busReceiverEntity.getDetailDataId());
                        ps.setTimestamp(2, new Timestamp(new Date().getTime()));
                        ps.setTimestamp(3, new Timestamp(new Date().getTime()));
                        ps.setString(4,(busReceiverEntity.getCrmProductId() ==null)?"":busReceiverEntity.getCrmProductId());
                        ps.setString(5,busReceiverEntity.getDeliverDate());
                        ps.setString(6, busReceiverEntity.getDeliverNum());
                        ps.setBigDecimal(7,busReceiverEntity.getDiscountPrice()==null?new BigDecimal(0):busReceiverEntity.getDiscountPrice());
                        ps.setString(8,busReceiverEntity.getExternalOrderId());
                        ps.setString(9,busReceiverEntity.getExternalSubOrderId()==null?"":busReceiverEntity.getExternalSubOrderId());
                        ps.setString(10,(busReceiverEntity.getIsSend()==null)?"":busReceiverEntity.getIsSend());
                        ps.setString(11,busReceiverEntity.getIsSync()==null?"":busReceiverEntity.getIsSync());
                        ps.setString(12,busReceiverEntity.getMilkmanLoginId()==null?"":busReceiverEntity.getMilkmanLoginId());
                        ps.setString(13,busReceiverEntity.getMilkmanPartyId()==null?"":busReceiverEntity.getMilkmanPartyId());
                        ps.setString(14, busReceiverEntity.getOperatorComment()==null?"":busReceiverEntity.getOperatorComment());
                        ps.setString(15,busReceiverEntity.getOperatorType()==null?"":busReceiverEntity.getOperatorType());
                        ps.setString(16,busReceiverEntity.getOrderId());
                        ps.setString(17,busReceiverEntity.getOrderItemSeqId());
                        ps.setBigDecimal(18, busReceiverEntity.getOrderPayment()==null?new BigDecimal(0):busReceiverEntity.getOrderPayment());
                        ps.setBigDecimal(19,busReceiverEntity.getOrderSalesAmount()==null?new BigDecimal(0):busReceiverEntity.getOrderSalesAmount());
                        ps.setBigDecimal(20,busReceiverEntity.getOrderTotalAmount()==null?new BigDecimal(0):busReceiverEntity.getOrderTotalAmount());
                        ps.setBigDecimal(21,busReceiverEntity.getDiscountAmount()==null?new BigDecimal(0):busReceiverEntity.getDiscountAmount());
                        ps.setString(22,busReceiverEntity.getProductId());
                        ps.setBigDecimal(23,busReceiverEntity.getUnitPrice()==null?new BigDecimal(0):busReceiverEntity.getUnitPrice());
                        ps.setInt(24,busReceiverEntity.getDailyPoints());
       

                    }
                }
        );
    }

    public void deleteAll(){
        String sql = "insert overwrite table milk_station select * from milk_station where 1=0";
        hiveJdbcTemplate.execute(sql);
    }
}
