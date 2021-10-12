package com.bigdata.service;

import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.spark.service.SparkService;
import com.bigdata.util.BigDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;



@Service
public class PaymentSettlementDataService {

    private String hdfsDirectory= HDFSPropertiesUtils.getProperty("hdfs.base.directory");

    @Autowired
    private BigDataUtil bigDataUtil;
    @Autowired
    SparkService sparkService;

    public void syncPayJournalToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);
    }

}
