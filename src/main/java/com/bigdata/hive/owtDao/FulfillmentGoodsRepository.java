package com.bigdata.hive.owtDao;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.stereotype.Service;

import com.bigdata.hive.entity.MaxRecordEntity;
import com.bigdata.mysql.entity.OrderDeliverDetailData;

@Service
public class FulfillmentGoodsRepository{

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;

    /**
     * <li>Description: TODO </li>
     */
    @PostConstruct
    public void createTable() {
        /*建表SQL语句*/
        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("fulfillment_goods ");
        sql.append("(" + 
        		"  id BIGINT, fulfillmentOrderId BIGINT, fulfillmentStatus STRING, predictDispatchAt TIMESTAMP, predictPushAt TIMESTAMP, predictDeliveryAt TIMESTAMP, outItemId STRING, goodsCode STRING, goodsName STRING," + 
        		"  breakupAt BIGINT, createdAt BIGINT, discountDetail STRING, customerId BIGINT, expectReportId BIGINT, goodsShortName STRING, goodsType INT, netPointId INT, omDiscountAmount INT, omDiscountType STRING," + 
        		"  omPrice INT, omPriceId INT, operationExtra STRING, orgId INT, price INT,  realReportId INT, shipTo INT, realInterceptResult BOOLEAN, expectInterceptResult BOOLEAN, isChangedGoods BOOLEAN, originGoodsCode INT," + 
        		"  originAmount BIGINT, paidAmount BIGINT, pointsAmount BIGINT, discountAmount BIGINT, noticeQuantity INT,  performQuantity INT, abnormalQuantity INT, abnormalType STRING, unit STRING, dispatchMode STRING," + 
        		"  outOrderLineId STRING, expressCompany STRING, expressNo STRING, deliveryAt TIMESTAMP, expressCompanyCode STRING," + 
        		"  warehouseId BIGINT, deliverymanId BIGINT, deliverymanName STRING, expressCompanyName STRING, updatedAt TIMESTAMP, express STRING"+           
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
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE fulfillment_goods ";
        hiveJdbcTemplate.execute(sql);
    }
    
    
    /**
     * <li>Description: TODO </li>
     *
     * @param pathFile TODO
     */
    public void loadTmpData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' OVERWRITE INTO TABLE tmp_fulfillment_goods ";
        hiveJdbcTemplate.execute(sql);
    }
    
    /**
     * <li>Description: TODO </li>
     *
     * @param pathFile TODO
     */
    public void loadData(String pathFile){
        String sql = "LOAD DATA INPATH  '"+pathFile+"' INTO TABLE fulfillment_goods ";
        hiveJdbcTemplate.execute(sql);
    }
    
    public String[] updateHisData() {
    	
    	//先把原表中需要更新的数据删除
    	String sql ="insert overwrite ali_prod_mysql.fulfillment_goods select g.* from ali_prod_mysql.fulfillment_goods g left join ali_prod_mysql.tmp_fulfillment_goods g1 on g1.id = g.id where g1.id is null; ";
//    	hiveJdbcTemplate.execute(sql);
    	//将临时表数据追加到原表的尾部。
    	sql += "insert into ali_prod_mysql.fulfillment_goods select * from ali_prod_mysql.tmp_fulfillment_goods";
    	return sql.split(";");
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
    	
    	
    	
    	
    	
    	
        hiveJdbcTemplate.update("insert into fulfillment_goods(id" + 
        		", fulfillmentOrderId" + 
        		", fulfillmentStatus" + 
        		", predictDispatchAt" + 
        		", predictPushAt" + 
        		", predictDeliveryAt" + 
        		", outItemId" + 
        		", goodsCode" + 
        		", goodsName" + 
        		", breakupAt" + 
        		", createdAt" + 
        		", discountDetail" + 
        		", customerId" + 
        		", expectReportId" + 
        		", goodsShortName" + 
        		", goodsType" + 
        		", netPointId" + 
        		", omDiscountAmount" + 
        		", omDiscountType" + 
        		", omPrice" + 
        		", omPriceId" + 
        		", operationExtra" + 
        		", orgId" + 
        		", price" + 
        		", realReportId" + 
        		", shipTo" + 
        		", realInterceptResult" + 
        		", expectInterceptResult" + 
        		", isChangedGoods" + 
        		", originGoodsCode" + 
        		", originAmount" + 
        		", paidAmount" + 
        		", pointsAmount" + 
        		", discountAmount" + 
        		", noticeQuantity" + 
        		", performQuantity" + 
        		", abnormalQuantity" + 
        		", abnormalType" + 
        		", unit" + 
        		", dispatchMode" + 
        		", outOrderLineId" + 
        		", expressCompany" + 
        		", expressNo" + 
        		", deliveryAt" + 
        		", expressCompanyCode" + 
        		", warehouseId" + 
        		", deliverymanId" + 
        		", deliverymanName" + 
        		", expressCompanyName" + 
        		", updatedAt" + 
        		", express)"
        		+ " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
        String sql = "insert overwrite table fulfillment_goods select * from fulfillment_goods where 1=0";
        hiveJdbcTemplate.execute(sql);
    }
    
    
    public String queryMaxIdAndCreatedTime(){
    	return "select max(id),max(createdat),max(updatedat) from ali_prod_mysql.fulfillment_goods where id is not null and createdat is not null and updatedat is not null;";
//    	return hiveJdbcTemplate.queryForObject(sql, String.class);
    }
    
    public TimeStamp queryLastUpdatedAt(String maxId){
    	String sql ="select max(updatedat) updatedTime from ali_prod_mysql.fulfillment_goods where id <="+maxId;
    	return hiveJdbcTemplate.queryForObject(sql, TimeStamp.class);
    }
    
    
}
