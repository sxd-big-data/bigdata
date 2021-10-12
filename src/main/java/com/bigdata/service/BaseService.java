package com.bigdata.service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.net.ntp.TimeStamp;
import org.springframework.beans.factory.annotation.Autowired;

import com.bigdata.common.ResponseMsg;
import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.kettle.service.KettleService;
import com.bigdata.spark.service.SparkService;

public abstract class BaseService {
	
	public final static String hdfsDirectory=HDFSPropertiesUtils.getProperty("hdfs.base.directory");
	
	@Autowired
	protected SparkService sparkService;

	@Autowired
	private KettleService kettleService;
	/**
	 * 从云端获取数据
	 * @param overWrite
	 * @param tableName
	 * @param hisTableName
	 * @return
	 */
	public Map<String,ResponseMsg> getDataByTransMeta(Boolean overWrite,String tableName,String hisTableName) {
		Map<String,ResponseMsg> resultMap = new HashMap();
		
		//获取履约单数据，并更新历史数据
		if(overWrite) {
			resultMap.put("result", kettleService.runKtr(tableName, null));
		}else {
			Map<String,String> params= new HashMap();
			//获取最大ID和最大创建时间
			String sql =getQueryMaxIdSql();
			Map map =sparkService.getMaxRecord("orderMaxIdAndCreatedTime",sql);
			//判断参数是否存在
			if(map.containsKey("id") && map.containsKey("createdat")) {
				params.put("id", map.get("id").toString());
				
				params.put("updatedat", map.get("createdat").toString().indexOf("-")>=0?
						map.get("createdat").toString().substring(0, map.get("createdat").toString().length()-2):
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date((long)map.get("createdat"))));
			}
			
			//获取新增加数据
			resultMap.put("result", kettleService.runKtr(tableName, params));
			//获取历史要更新的记录
//			hisTableName = tableName+"_his";
//			resultMap.put("result1", kettleService.runKtr(hisTableName, params));
			
		}
		
		return resultMap;
	}
	
	public Map<String,ResponseMsg> getDataByTransMetaDate(Boolean overWrite,String tableName,String hisTableName,String startDate,String endDate) {
		Map<String,ResponseMsg> resultMap = new HashMap();
		Map<String,String> params= new HashMap();
		 params.put("startDate", startDate);
	     params.put("endDate", endDate);
		if("".equals(startDate) && "".equals(endDate)) {
			Calendar nowDate = Calendar.getInstance();
			nowDate.set(Calendar.MONTH,-1);
			nowDate.set(Calendar.DAY_OF_MONTH,1);
	        startDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate.getTime());
	        nowDate.set(Calendar.MONTH,+1);
	        endDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate.getTime());
	        System.out.println("startDate:"+startDate+":::::::endDate:"+endDate);
	        params.put("startDate", startDate);
	        params.put("endDate", endDate);
		}
		//获取履约单数据，并更新历史数据
		if(overWrite) {
			resultMap.put("result", kettleService.runKtr(tableName, params));
		}else {
			//获取最大ID和最大创建时间
			String sql =getQueryMaxIdSql();
			Map map =sparkService.getMaxRecord("orderMaxIdAndCreatedTime",sql);
			//判断参数是否存在
			if(map.containsKey("id") && map.containsKey("createdat")) {
				params.put("id", map.get("id").toString());
				
				params.put("updatedat", map.get("createdat").toString().indexOf("-")>=0?
						map.get("createdat").toString().substring(0, map.get("createdat").toString().length()-2):
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date((long)map.get("createdat"))));
			}
			
			//获取新增加数据
			resultMap.put("result", kettleService.runKtr(tableName, params));
			//获取历史要更新的记录
//			hisTableName = tableName+"_his";
//			resultMap.put("result1", kettleService.runKtr(hisTableName, params));
			
		}
		
		return resultMap;
	}
	
	public abstract String getQueryMaxIdSql();
	
	public abstract void syncMysqlToHdfs(Boolean overWrite,String tableName);
	public abstract void syncMysqlToHdfs(Boolean overWrite,String tableName,String startDate,String endDate);
}
