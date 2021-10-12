package com.bigdata.util;

import com.bigdata.common.ResponseMsg;
import com.bigdata.hadoop.utils.HDFSUtils;
import com.bigdata.service.BigDataUtilSerice;
import com.bigdata.kettle.service.KettleService;
import com.bigdata.spark.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
@Service
public class BigDataUtil {

    @Autowired
    private SparkService sparkService;
    @Autowired
    private KettleService kettleService;
    @Autowired
    private BigDataUtilSerice bigDataUtilSerice;


    public    Map<String, ResponseMsg> getDataByTransMeta(Boolean overWrite, String tableName, String hisTableName) {
        Map<String,ResponseMsg> resultMap = new HashMap();
        String sql =bigDataUtilSerice.queryMaxIdAndCreatedTime(tableName);
        Map map = getDataByTransMeta(resultMap,overWrite, tableName, hisTableName,"tablesMaxIdAndCreatedTime",
                sql);
        return map;
    }

    public    Map<String, ResponseMsg> getDataByTransMeta(Map<String,ResponseMsg> resultMap,Boolean overWrite, String tableName, String hisTableName,String appName, String sql) {


        //获取履约单数据，并更新历史数据
        if(overWrite) {
            resultMap.put("result", kettleService.runKtr(tableName, null));
        }else {
            Map<String,String> params= new HashMap();
            //获取最大ID和最大创建时间

            Map map =sparkService.getMaxRecord(appName,sql);
            //判断参数是否存在
            if(map.containsKey("id") && map.containsKey("createdat")) {
                params.put("id", map.get("id").toString());
                params.put("updatedat", map.get("createdat").toString().substring(0, map.get("createdat").toString().length()-2));
            }

            //获取新增加数据
            resultMap.put("result", kettleService.runKtr(tableName, params));
            //获取历史要更新的记录
            resultMap.put("result1", kettleService.runKtr(hisTableName, params));

        }
        return resultMap;
    }

    public Map<String, ResponseMsg> getRunKtr(Map<String,ResponseMsg> resultMap,String tableName){
        resultMap.put("result", kettleService.runKtr(tableName, null));
        return resultMap;
    }

    public void syncTableToHdfs(String hdfsDirectory,String tableName){
        ResponseMsg result =null;
        ResponseMsg result1 =null;
        Map<String,ResponseMsg> resultMap = new HashMap();
        Map map = getRunKtr(resultMap,tableName);
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
        try {
            if(HDFSUtils.existFile(uploadFilePath)) {
                //将数据导入到临时表
                getLoadOverData(tableName,uploadFilePath);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void getLoadOverData(String tableNmae, String uploadFilePath){
        bigDataUtilSerice.getLoadOverData(tableNmae,uploadFilePath);
    }

    public void syncTableHisToHdfs(Boolean overWrite,String hdfsDirectory, String tableName){
        ResponseMsg result =null;
        ResponseMsg result1 =null;
        String hisTableName = "";
        if(!overWrite){
            hisTableName = tableName+"his";
        }
        //获取数据
        Map map = getDataByTransMeta(overWrite, tableName, hisTableName);

        //首先从阿里云重新获取履约单
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if(overWrite) {
            try {
                if(HDFSUtils.existFile(uploadFilePath)) {
                    //将数据导入到临时表
                    getLoadOverData(tableName,uploadFilePath);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }else {

            if(!hisTableName.isEmpty()) {
                uploadFilePath=hdfsDirectory+hisTableName+".txt";
                try {
                    HDFSUtils.uploadFile(hisTableName);
                    if(HDFSUtils.existFile(uploadFilePath)) {
                        //创建临时表
                        bigDataUtilSerice.createTmpTable(tableName);
                        //将数据导入到临时表
                        bigDataUtilSerice.loadTmpData(tableName,uploadFilePath);
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                //历史记录更新操作
               bigDataUtilSerice.updateHisData(tableName);
            }

            try {
                uploadFilePath=hdfsDirectory+tableName+".txt";
                if(HDFSUtils.existFile(uploadFilePath)) {
                    //将数据导入到临时表
                    bigDataUtilSerice.loadData(tableName,uploadFilePath);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
