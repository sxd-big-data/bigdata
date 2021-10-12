package com.bigdata.service;

import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.util.BigDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CouponService {
    private String hdfsDirectory= HDFSPropertiesUtils.getProperty("hdfs.base.directory");
    @Autowired
    private BigDataUtil bigDataUtil;


    public void syncCouponResourceToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);
    }

    public void syncPromotionToolToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncTreeActivityToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncSimpleActivityToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

}
