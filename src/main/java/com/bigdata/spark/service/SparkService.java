package com.bigdata.spark.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import com.bigdata.spark.utils.SparkUtil;

@Service
public class SparkService {

	
	public Object queryObject(String sql) {
		
		
		return null;
	}
	
	
	public Map<String,Object> getMaxRecord(String appName,String sql) {
		//定义输出的map
		Map map = new HashedMap();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		System.setProperty("USER_NAME", "hadoop");
		SparkSession sparkSession =SparkUtil.createSparkClusterSession(appName);
		
		Dataset result =sparkSession.sql(sql);
		List<Row> list = result.collectAsList();
    	Row row = list.get(0);
    	map.put("id", row.get(0).toString());
//    	map.put("upatedat", row.get(1).toString());
    	map.put("createdat", row.get(1));
    	System.out.println("时间"+row.get(1).toString());
//    	map.put("createdAt", new Timestamp() row.get(1).toString()));
    	//获取
    	return map;
    }
	
	/**
	 * 更新历史记录
	 * @param sql
	 * @return
	 */
	public String updateHisData(String appName,String sql) {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		System.setProperty("USER_NAME", "hadoop");
		SparkSession sparkSession =SparkUtil.createSparkClusterSession(appName);
		sparkSession.sql(sql);
		return "success";
	}
	
	
}
