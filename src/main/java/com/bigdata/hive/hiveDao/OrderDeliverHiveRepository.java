package com.bigdata.hive.hiveDao;

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
public class OrderDeliverHiveRepository{

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * <li>Description: TODO </li>
     */
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
//        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
//        sql.append("order_deliver_detail_data ");
//        sql.append("(id STRING comment '主键ID' " +
//                ",create_time STRING  comment '创建时间' " +
//                ",update_time STRING comment '更新时间' " +
//                ",crm_product_id STRING comment 'CRM产品ID' " +
//                ",deliver_date STRING  comment '配送日期'  " +
//                ",deliver_num STRING comment '配送份数' " +
//                ",discount_price DECIMAL  comment '折扣单价'  " +
//                ",external_order_id STRING comment '计划订单ID' " +
//                ",external_sub_order_id STRING  comment '子单号'  " +
//                ",is_send STRING comment '是否通知发货' " +
//                ",is_sync STRING  comment '是否同步成功'  " +
//                ",milkman_login_id STRING comment '送奶员登录ID' " +
//                ",milkman_party_id STRING  comment '送奶员组织ID' " +
//                ",operator_comment STRING comment '操作说明' " +
//                ",operator_type STRING  comment '操作类型' " +
//                ",order_id STRING comment '订单号' " +
//                ",order_item_seq_id STRING  comment '订单项ID' " +
//                ",order_payment STRING comment '订单支付金额' " +
//                ",order_sales_amount STRING  comment '订单销售金额' " +
//                ",order_total_amount STRING  comment '订单总金额' " +
//                ",discount_amount STRING comment '折扣金额' " +
//                ",product_id STRING  comment '产品ID' " +
//                ",unit_price STRING comment '单价' " +
//                ",daily_points STRING comment '赠送积分' " +              
//                ") ");
//        sql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"); // 定义分隔符  
//        sql.append(" STORED AS TEXTFILE"); // 作为文本存储*/
//        hiveJdbcTemplate.execute(sql.toString());
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
    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE order_deliver_detail_data ";
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
    	
        hiveJdbcTemplate.update("insert into order_deliver_detail_data(id, create_time, update_time, crm_product_id, deliver_date, "
        		+ "deliver_num, discount_price, external_order_id, external_sub_order_id, is_send, "
        		+ "is_sync, milkman_login_id, milkman_party_id, operator_comment, operator_type, "
        		+ "order_id, order_item_seq_id, order_payment, order_sales_amount, order_total_amount, "
        		+ "discount_amount, product_id, unit_price, daily_points)"
        		+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
        String sql = "insert overwrite table order_deliver_detail_data select * from order_deliver_detail_data where 1=0";
        hiveJdbcTemplate.execute(sql);
    }
}
