package com.bigdata.spark.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.bigdata.hadoop.utils.HDFSUtils;
import com.bigdata.service.OrderService;
import com.bigdata.spark.service.SparkService;
import com.bigdata.spark.sql.ExecSparkSql;
import com.bigdata.spark.sql.SparkSqlContent;
import com.bigdata.spark.sql.SparkSqlFactory;
import com.bigdata.spark.utils.ParamValidateUtil;
import com.bigdata.spark.utils.SparkUtil;


//@Api(value = "spark执行controller", tags = { "spark执行操作接口" })
@RestController
@RequestMapping("/spark")
public class SparkSqlController {

	/**
	 * 执行sql入口方法 需传入2个参数：sql文件完整路径，保存csv文件路径 1. 读取sql文件，解析sql 2. 执行sql，生成csv
	 * 
	 * @param 、args
	 */
	
	@Autowired
	private SparkService sparkService;

	

	
	
	
//	@ApiOperation("spark执行接口")
//	@ApiImplicitParam(name = "paramsList", value = "参数列表", dataType = "String", paramType = "query", required = true)
	@GetMapping("/execSparkSqlFile")
	public void execSparkSqlFile(List<String> paramsList) { // @ApiParam(name="参数列表",value =
																			// "[\"/path\",\"/path2\"]",required = true)
		String msg = ParamValidateUtil.checkParam(paramsList, false, 2, true);
		if (!ParamValidateUtil.passValid(msg)) {
			System.out.println("==================== " + msg);
			return;
		}
		System.out.println("=================== 开始执行");
		StringBuffer sb = new StringBuffer();
		for (String s : paramsList) {
			sb.append(s + ", ");
		}
		System.out.println("传入参数信息：" + sb.toString().substring(0, sb.length() - 1));
		long st = System.currentTimeMillis();
		SparkSession spark = SparkUtil.createSparkClusterSession("ExecSparkSql");
		int paramCnt = 0;
		List<String> params = new ArrayList<>();
		if ((paramCnt = paramsList.size()) > 2) {
			for (int i = 2; i < paramCnt; i++) {
				params.add(paramsList.get(i));
			}
		}
		// 创建sparksql,通过外部sql文件
		ExecSparkSql sql = SparkSqlFactory.buildExecSparkSqlFromFile(paramsList.get(0), spark, params);

		// 创建reader
		DataFrameReader mallReadr = SparkUtil.createReader("prod.mall", spark);
		DataFrameReader owtReadr = SparkUtil.createReader("prod.owt", spark);

		if (null != sql.getPrepareSql() && sql.getPrepareSql().size() > 0) {
			for (SparkSqlContent sqlContent : sql.getPrepareSql()) {
				if ("prod.owt".equalsIgnoreCase(sqlContent.getDbName())) {
					Dataset ds = owtReadr.option("dbtable", sqlContent.getSqlStr()).load();
//                    ds.show();
					ds.createOrReplaceTempView(sqlContent.getAlias());
				}
				if ("prod.mall".equalsIgnoreCase(sqlContent.getDbName())) {
					Dataset ds = mallReadr.option("dbtable", sqlContent.getSqlStr()).load();
//                    ds.show();
					ds.createOrReplaceTempView(sqlContent.getAlias());
				}
			}
		} else {
			System.out.println("===================不需要预先执行准备数据的sql");
		}
		if (null != sql.getExecSql()) {
			System.out.println("================开始执行最终sql");
			Dataset<Row> ds = spark.sql(sql.getExecSql().getSqlStr());
//            StructType schema = ds.schema();
//            System.out.println("==================== " + schema.toString());
			// 解析文件目录和文件名
			String filePath = paramsList.get(1).substring(0, paramsList.get(1).lastIndexOf("/"));
			String fileName = paramsList.get(1).substring(paramsList.get(1).lastIndexOf("/") + 1,
					paramsList.get(1).length());
			try {
				HDFSUtils.createFile((MultipartFile) new File(fileName));
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// 服务器执行
			ds.repartition(1).write().option("header", true).csv(filePath.concat("/" + fileName));
			// 本地测试
//            ds.repartition(1).write().option("header", true).csv("D:\\sparkout\\itemprice");
//            List<Row> list = ds.javaRDD().collect();
//            System.out.println(list.get(0));
			// 使用指定文件名重命名文件
			try {
				HDFSUtils.renameFile(filePath, filePath + "/" + fileName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		spark.stop();
		System.out.println("========================== 执行结束，耗时：" + (System.currentTimeMillis() - st) + " 毫秒");
	}
	
	


	
	
//	@ApiOperation("sparkSql执行接口")
	@GetMapping("/testSQL")
	public String testSQL(String startDate,String endDate,String goodsCode) {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		System.setProperty("USER_NAME", "hadoop");
		SparkSession sparkSession =SparkUtil.createSparkClusterSession("TestSql");
	
		Dataset result =sparkSession.sql("select * from ali_prod_mysql.fulfillment_goods limit 10");
		
		Dataset<Row> ds =sparkSession.sql("SELECT\r\n" + 
        		"       o.id AS orderId,\r\n" + 
        		"       acl_pp.biz_name AS areaName,\r\n" + 
        		"       acl_p.biz_name AS cityName,\r\n" + 
        		"       acl_a.biz_name AS regionName,\r\n" + 
        		"       o.milkStationName AS milkStationName,\r\n" + 
        		"       o.outOrderId AS outOrderId,\r\n" + 
        		"       o.createdat AS createdAt,\r\n" + 
        		"       (CASE WHEN o.operatorType = 'NORMAL' THEN '线上订单' WHEN o.operatorType = 'DELIVERER' THEN '送奶工' WHEN o.operatorType = 'STATION_MASTER' THEN '块长' WHEN o.operatorType = 'CUSTOMER_SERVICE' THEN '客服代下单' WHEN o.operatorType = 'PLAN_ORDER' THEN '计划下单' WHEN o.operatorType = 'GROUP_PERIOD' THEN '团客户开单' END) AS orderSource,\r\n" + 
        		"       o.subChannelName subChannelName,\r\n" + 
        		"       g.goodsName AS goodsName,\r\n" + 
        		"       g.noticeQuantity AS noticeQuantity,\r\n" + 
        		"       g.paidamount/100 AS price,\r\n" + 
        		"       o.fullAddress AS fullAddress,\r\n" + 
        		"       o.demandSideMobile AS demandSideMobile,\r\n" + 
        		"       o.demandSideName AS demandSideName,\r\n" + 
        		"       o.demandSideContact AS demandSideContact,\r\n" + 
        		"      MIN(g.predictDeliveryAt) AS startDeliveryAt,\r\n" + 
        		"      MAX(g.predictDeliveryAt) AS endDeliveryAt,\r\n" + 
        		"       (CASE o.deliveryType WHEN 'MORNING' THEN '晨配' ELSE '夜配' END) AS deliveryType,\r\n" + 
        		"       o.deliverymanName AS deliverymanName,\r\n" + 
        		"       (o.originamount/100) AS totalOriginAmount,\r\n" + 
        		"       (o.paidamount/100) AS totalPaidAmount,\r\n" + 
        		"      (CASE o.auditStatus WHEN 'AUDITED' THEN '已审核' WHEN 'CLOSED' THEN '已关闭' ELSE '等待中' END) AS auditStatus,\r\n" + 
        		"    (CASE o.fulfillmentstatus WHEN 'PENDING' THEN '等待中' WHEN 'BREAKUP' THEN '已终止' ELSE '已处理' END) AS fulfillmentStatus\r\n" + 
        		"FROM fulfillment_order o INNER JOIN fulfillment_goods g ON o.id = g.fulfillmentOrderId\r\n" +
        		"         LEFT JOIN milk_station m ON o.milkStationId = m.id\r\n" +
        		"         LEFT JOIN acl_tree_node acl_a ON m.id = acl_a.key\r\n" +
        		"         LEFT JOIN acl_tree_node acl_p ON acl_a.parent_key = acl_p.key\r\n" +
        		"         LEFT JOIN acl_tree_node acl_pp ON acl_p.parent_key = acl_pp.key\r\n" +
        		"WHERE o.outOrderType = 'PERIOD' AND o.type = 'SALE_ORDER'\r\n" + 
        		"AND o.fulfillmentStatus <> 'BREAKUP'\r\n" + 
        		" AND o.createdAt >= '"+startDate+"'\r\n" + 
        		" AND o.createdAt < '"+startDate+"'\r\n" + 
        		"AND g.goodsCode  IN ("+goodsCode+")\r\n" + 
        		"AND o.operatorType = 'NORMAL'\r\n" + 
        		" GROUP BY o.id,acl_pp.biz_name,acl_p.biz_name,acl_a.biz_name,milkStationName,outOrderId,o.createdAt,orderSource,subChannelName,goodsName,noticeQuantity,g.paidamount,fullAddress,demandSideMobile,demandSideName,demandSideName,demandSideContact,deliveryType\r\n" + 
        		" ,o.deliverymanName,totalOriginAmount,o.paidamount,auditStatus,o.fulfillmentStatus");
		
		List<Row> list = ds.collectAsList();
//		for(Row row:list) {
//			System.out.println(row.get(0).toString());
//		}
		try {
			SparkUtil.writeCsvToDirectory(ds,new Path("hdfs://node3:9000/user/root/directory/data/goods_delivery_data"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HDFSUtils.copyRename("/user/root/directory/data/goods_delivery_data","/user/root/directory/data/goods_delivery_data/goods_delivery_data.csv");
		
//		sparkSession.stop();
		return String.valueOf(list.size());

	}
	
	


	
}
