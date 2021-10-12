package com.bigdata.kettle.service;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;

import com.bigdata.common.ResponseMsg;
import com.bigdata.hadoop.utils.HDFSPropertiesUtils;


@Service
public class KettleService {

	private String dirPath=HDFSPropertiesUtils.getProperty("kettle.script.path");
	
	private String rootPath = System.getProperty("user.dir");

	/**
	 * 执行ktr文件
	 * 
	 * @param filename
	 * @param params
	 * @return
	 */
	
	
	/**
     * 生成一个转化,把一个数据库中的数据转移到另一个数据库中,只有两个步骤,
     *      第一个是表输入,
     *      第二个是表插入与更新操作
     * @return
     * @throws KettleXMLException
     */
    public TransMeta generateMyOwnTrans() {
        TransMeta transMeta = new TransMeta();
        //设置转化的名称
        transMeta.setName("insert_update");
        //添加转换的数据库连接
        DatabaseMeta dataMeta = new DatabaseMeta("db1", "MySQL", "Native","172.16.2.213", "demo", "3306", "root", "y8m2a0i7jd");
        transMeta.addDatabase(dataMeta);
        DatabaseMeta targetMeta = new DatabaseMeta("db2", "MySQL", "Native","172.16.2.213", "demo", "3306", "root", "y8m2a0i7jd");
        transMeta.addDatabase(targetMeta);
        //registry是给每个步骤生成一个标识id
        PluginRegistry registry = PluginRegistry.getInstance();
        //第一个表输入步骤(TableInputMeta) 
        TableInputMeta tableInput = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
        //给表输入添加一个DatabaseMeta连接数据库
        DatabaseMeta database_bjdt = transMeta.findDatabase("db1");
        tableInput.setDatabaseMeta(database_bjdt);
        String selectSQL = "SELECT id,name FROM test1";
        tableInput.setSQL(selectSQL);
        //添加TableInputMeta到转换中
        StepMeta tableInputMetaStep = new StepMeta(tableInputPluginId, "TableInput", tableInput);
        //给步骤添加在spoon工具中的显示位置
        tableInputMetaStep.setDraw(true);
        tableInputMetaStep.setLocation(100, 100);
        transMeta.addStep(tableInputMetaStep);

        //第二个步骤插入与更新
        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
        String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
        //添加数据库连接
        
        DatabaseMeta database_kettle = transMeta.findDatabase("db2");
        insertUpdateMeta.setDatabaseMeta(database_kettle);
        //设置操作的表
        insertUpdateMeta.setTableName("test2");
        //设置用来查询的关键字
        insertUpdateMeta.setKeyLookup(new String[]{"id"});
        insertUpdateMeta.setKeyStream(new String[]{"id"});
        insertUpdateMeta.setKeyStream2(new String[]{""});
        insertUpdateMeta.setKeyCondition(new String[]{"="});
        //设置要更新的字段
        String[] updatelookup = {"id","name"} ;
        String[] updateStream = {"id","name"} ;
        Boolean[] updateOrNot = {false,true};
        insertUpdateMeta.setUpdateLookup(updatelookup);
        insertUpdateMeta.setUpdateStream(updateStream);
        insertUpdateMeta.setUpdate(updateOrNot);
        //添加步骤到转换中
        StepMeta insertUpdateStep = new StepMeta(insertUpdateMetaPluginId, "TableOutput", insertUpdateMeta);
        insertUpdateStep.setDraw(true);
        insertUpdateStep.setLocation(250, 100);
        transMeta.addStep(insertUpdateStep);
        //添加hop把两个步骤关联起来
        transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, insertUpdateStep));
        return transMeta;
    }
	
    
    /**
     * 上述操作将会产生一个ktr文件,接下来的操作是对ktr文件进行转换：
     */
	
	public String runKtr(TransMeta transMeta) throws KettleException { // 初始化ketlle
		KettleEnvironment.init(); // 创建转换元数据对象
//		SlaveServer slaveServer = new SlaveServer("222i159","localhost"
//                ,"8081","cluster","cluster");
		TransMeta meta = generateMyOwnTrans();//new TransMeta("E:\\test_boot_workspace\\kettle-test\\update_insert_Trans.ktr");
		Trans trans = new Trans(meta);
		trans.prepareExecution(null);
		trans.startThreads();
		trans.waitUntilFinished();
		if (trans.getErrors() == 0) {
			System.out.println("执行成功！");
		}
		return "success";
	}
	
	
	/*
	 * public String runKtr(String ktrName,) throws KettleException { // 初始化ketlle
	 * KettleEnvironment.init(); // 创建转换元数据对象 TransMeta meta = new
	 * TransMeta("E:\\test_boot_workspace\\kettle-test\\update_insert_Trans.ktr");
	 * Trans trans = new Trans(meta); trans.prepareExecution(null);
	 * trans.startThreads(); trans.waitUntilFinished(); if (trans.getErrors() == 0)
	 * { System.out.println("执行成功！"); } return "success"; }
	 */
    
	public ResponseMsg runKtr(String filename, Map<String, String> params) {
		ResponseMsg response = ResponseMsg.getSuccessMsg();
		try {
			File file = ResourceUtils.getFile("classpath:"+dirPath + filename+".ktr");
//			String filePath =ClassLoader.getSystemResources(dirPath + filename+".ktr")
			TransMeta tm = new TransMeta(file.getPath());
			Trans trans = new Trans(tm);
			if (params != null) {
				Iterator<Map.Entry<String, String>> entries = params.entrySet().iterator();
				while (entries.hasNext()) {
					Map.Entry<String, String> entry = entries.next();
					trans.setParameterValue(entry.getKey(), entry.getValue());
				}
			}

			trans.execute(null);
			trans.waitUntilFinished();
//			response.setHead(ResponseHead.buildSuccessHead());
		} catch (Exception e) {
			response = ResponseMsg.getFailMsg();
			e.printStackTrace();
			String er = e.getMessage();
//			response.setHead(ResponseHead.buildFailedHead(er));

		}

		return response;
	}
	  
	 	/**
		 * 执行kjb文件
		 * 
		 * @param filename
		 * @param params
		 * @return
		 */
		public ResponseMsg runKjb(String filename, Map<String, String> params) {
			final ResponseMsg response = new ResponseMsg<>();
			try {
				JobMeta jm = new JobMeta(dirPath + filename, null);
				Job job = new Job(null, jm);
				if (params != null) {
					Iterator<Map.Entry<String, String>> entries = params.entrySet().iterator();
					while (entries.hasNext()) {
						Map.Entry<String, String> entry = entries.next();
						job.setVariable(entry.getKey(), entry.getValue());
					}
				}

				job.start();
				job.waitUntilFinished();
//				response.setHead(ResponseHead.buildSuccessHead());
			} catch (Exception e) {
				e.printStackTrace();
				String er = e.getMessage();
//				response.setHead(ResponseHead.buildFailedHead(er));

			}
			return response;
		}
			 

	public static KettleDatabaseRepository repository;
	public static DatabaseMeta databaseMeta;
	public static KettleDatabaseRepositoryMeta kettleDatabaseMeta;
	public static RepositoryDirectoryInterface directory;

	/*
	 * KETTLE初始化
	 */
	public static String KettleEnvironments() {
		try {
			KettleEnvironment.init();
			repository = new KettleDatabaseRepository();
			databaseMeta = new DatabaseMeta("ETL", "Oracle", "Native", "172.14.5.6", "cdr", "1521", "ETL", "xin");// 资源库数据库地址，我这里采用oracle数据库
			kettleDatabaseMeta = new KettleDatabaseRepositoryMeta("ETL", "ERP", "Transformation description",
					databaseMeta);
			repository.init(kettleDatabaseMeta);
			repository.connect("adm", "adm");// 资源库用户名和密码
			directory = repository.loadRepositoryDirectoryTree();
		} catch (KettleException e) {
			e.printStackTrace();
			return e.getMessage();
		}
		return null;
	}
}