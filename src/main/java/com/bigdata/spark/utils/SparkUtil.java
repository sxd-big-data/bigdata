package com.bigdata.spark.utils;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.util.ResourceUtils;

import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.spark.db.PropsFactory;

public class SparkUtil {
	
	
	private static String jarBasePath = HDFSPropertiesUtils.getProperty("jar.base.path");
    public static SparkSession createSparkSession(String appName) {
        SparkSession spark = SparkSession.builder().appName(appName).master("local").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
    
    public static SparkSession createSparkClusterSession(String appName) {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		System.setProperty("USER_NAME", "hadoop");
    	File directory = new File("");// 参数为空
    	String rootPath = System.getProperty("user.dir");
        MavenXpp3Reader reader = new MavenXpp3Reader();
        String myPom = rootPath + File.separator + "pom.xml";
        Model model;
        SparkSession spark=null;
    	try {
			//获取项目根目录绝对路径
			String courseFile = directory.getCanonicalPath();
			
			model = reader.read(new FileReader(myPom));
			String version = model.getVersion();
			String artifactId = model.getArtifactId();
			SparkConf conf = new SparkConf();
			conf.set("spark.jars", jarBasePath+artifactId+"-"+version+".jar")
//            .set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
			.set("spark.app.name", "BIG_DATA")
            .set("spark.master", "spark://node3:7077")
            .set("spark.driver.host", "JSL72D7JFT1C939")
            .set("spark.executor.memory", "1024M")
            .set("spark.eventLog.enabled", "true")
            .set("spark.debug.maxToStringFields", "100")
//            .set("main.class","com.bigdata.spark.controller.SparkSqlController")
            .set("hive.metastore.uris","thrift://node3:9083")
            .set("spark.eventLog.dir", "hdfs://node3:9000/spark/history")
            .set("spark.sql.warehouse.dir ","hdfs://node3:9000/user/root/warehouse");
			
	        spark = SparkSession
	        		.builder()
	        		.config(conf)
	        		.enableHiveSupport().getOrCreate();
	        spark.sparkContext().setLogLevel("WARN");
    	}catch (Exception e) {
			// TODO: handle exception
		}
        return spark;
    }

    public static DataFrameReader createReader(String dbPrefix, SparkSession spark) {
        Map<String, String> propsMap = PropsFactory.createMysqlPropsMap(dbPrefix);
        return spark.sqlContext().read().format("jdbc").options(propsMap);
    }
    
    /**
     * 将查询结果数据存到CSV文件，并保存到hdfs
     * @param dataFrame
     * @param directory
     */
    public static void writeCsvToDirectory(Dataset<Row> dataFrame, Path directory) {
		dataFrame.repartition(1)
		.write()
		.format("csv")
		.option("header", "true")
		.option("delimiter", ",")
//        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
		.mode(SaveMode.Overwrite)
		.save(directory.toString());
    }
}
