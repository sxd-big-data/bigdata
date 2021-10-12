package com.bigdata.hadoop.utils;


import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.texen.util.PropertiesUtil;
import org.springframework.beans.factory.annotation.Value;

public class HDFSPropertiesUtils {
	private static Properties props;
	
	static {
		
		String fileName = "application.properties";

		props = new Properties();
		try {
			props.load(new InputStreamReader(PropertiesUtil.class
					.getClassLoader().getResourceAsStream(fileName), "UTF-8"));
		} catch (IOException e) {
			System.out.println("配置文件读取异常");
		}
	}

	/***
	 * 
	 * @param key
	 *            键值
	 * @return 返回获取结果
	 */
	public static String getProperty(String key) {
		String value = props.getProperty(key.trim());
		// 判断value是否为空，对于isBlank而言""， " "， "      "， null 都返回为空
		if (StringUtils.isBlank(value)) {
			return null;
		}
		return value.trim();
	}

	/**
	 * 
	 * @param key
	 *            键值
	 * @param defaultValue
	 *            如果未找到相应的value值，则以defaultValue代替
	 * @return 返回获取结果
	 */
	public static String getProperty(String key, String defaultValue) {

		String value = props.getProperty(key.trim());
		if (StringUtils.isBlank(value)) {
			value = defaultValue;
		}
		return value.trim();
	}
	
	
	public static String getPath() {
		String key = "hdfs.fs.path",defaultValue="hdfs://node3:9000";
		String value = props.getProperty(key.trim());
		if (StringUtils.isBlank(value)) {
			value = defaultValue;
		}
		return value.trim();
	}
	
	public static String getUserName() {
		String key = "hdfs.username",defaultValue="hadoop";
		String value = props.getProperty(key.trim());
		if (StringUtils.isBlank(value)) {
			value = defaultValue;
		}
		return value.trim();
	}
	
	public static void main(String[] args){
	   System.out.println(HDFSPropertiesUtils.getProperty("dll.path"));
	}
}
