package com.bigdata.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bigdata.common.ResponseMsg;
import com.bigdata.hadoop.utils.HDFSUtils;
import com.bigdata.hive.owtDao.FulfillmentGoodsRepository;
import com.bigdata.hive.owtDao.MilkSationRepository;

@Service
public class MilkSationService extends BaseService{
	
	

	@Autowired
	private MilkSationRepository milkSationRep;


	
	//调用接口生成导出数据，判断当前表是否需要增量导入，增量导入时判断数据最大记录及时间，先根据时间判断历史数据是否存在更新，
	//如果存在更新则先进行，更新数据替换，然后根据最大ID获取新增数据
	
	public void syncMysqlToHdfs(Boolean overWrite,String tableName){
		ResponseMsg result =null;
		Map<String,String> params= new HashMap();
		String hisTableName = "";
		if(!overWrite) {
			hisTableName=tableName+"_his";
		}
		//获取数据
		Map map = getDataByTransMeta(overWrite, tableName, hisTableName);
		
		//首先从阿里云重新获取履约单
		result = map.containsKey("result")?(ResponseMsg) map.get("result"):null;
		if(result !=null && !result.isSuccess()) {
			return ;
		}
		
		String uploadFilePath=hdfsDirectory+tableName+".txt";
		try {
			HDFSUtils.uploadFile(tableName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		if(overWrite) {
			try {
				if(HDFSUtils.existFile(uploadFilePath)) {
					//将数据导入到临时表
					milkSationRep.loadOverData(uploadFilePath);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}


	@Override
	public String getQueryMaxIdSql() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void syncMysqlToHdfs(Boolean overWrite, String tableName, String startDate, String endDate) {
		// TODO Auto-generated method stub
		
	}
	
	
	

}
