package com.bigdata.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bigdata.common.ResponseMsg;
import com.bigdata.hadoop.utils.HDFSUtils;
import com.bigdata.hive.owtDao.FulfillmentGoodsRepository;

@Service
public class GoodsService extends BaseService{
	
	

	@Autowired
	private FulfillmentGoodsRepository goodsRep;


	
	//调用接口生成导出数据，判断当前表是否需要增量导入，增量导入时判断数据最大记录及时间，先根据时间判断历史数据是否存在更新，
	//如果存在更新则先进行，更新数据替换，然后根据最大ID获取新增数据
	
	public void syncMysqlToHdfs(Boolean overWrite,String tableName,String startDate,String endDate){
		ResponseMsg result =null;
		ResponseMsg result1 =null;
		Map<String,String> params= new HashMap();
		String hisTableName = "";
		if(!overWrite) {
			hisTableName=tableName+"_his";
		}
		//获取数据
		Map map = getDataByTransMetaDate(overWrite, tableName, hisTableName,startDate,endDate);
//		
//		//首先从阿里云重新获取履约单
		result = map.containsKey("result")?(ResponseMsg) map.get("result"):null;
		if(result !=null && !result.isSuccess()) {
			return ;
		}
		result1 = map.containsKey("result1")?(ResponseMsg) map.get("result1"):null;
		if(result1 !=null &&  !result1.isSuccess() ) {
			return ;
		}
		String uploadFilePath=hdfsDirectory+tableName+".txt";
		try {
			HDFSUtils.uploadFile(tableName);
		} catch (Exception e) {
//			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		if(overWrite) {
			try {
				if(HDFSUtils.existFile(uploadFilePath)) {
					//将数据导入到临时表
					goodsRep.loadOverData(uploadFilePath);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			
			try {
				if(HDFSUtils.existFile(uploadFilePath)) {
					//将数据导入到临时表
					goodsRep.loadData(uploadFilePath);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//
			/*if(!hisTableName.isEmpty()) {
				uploadFilePath=hdfsDirectory+hisTableName+".txt";
				try {
					HDFSUtils.uploadFile(hisTableName);
					if(HDFSUtils.existFile(uploadFilePath)) {
						//将数据导入到临时表
						goodsRep.loadTmpData(uploadFilePath);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//历史记录更新操作
				String[] sql =goodsRep.updateHisData();
				//删除要更新的记录
				sparkService.updateHisData(hisTableName+"Update", sql[0]);
				//将要更新的最新记录写入库
				sparkService.updateHisData(hisTableName+"Update", sql[1]);
			}*/
		}

	}


	@Override
	public String getQueryMaxIdSql() {
		// TODO Auto-generated method stub
		return goodsRep.queryMaxIdAndCreatedTime();
	}


	@Override
	public void syncMysqlToHdfs(Boolean overWrite, String tableName) {
		// TODO Auto-generated method stub
		
	}


}
