package com.bigdata.service;

import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.util.BigDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProductService {
    private String hdfsDirectory= HDFSPropertiesUtils.getProperty("hdfs.base.directory");
    @Autowired
    private BigDataUtil bigDataUtil;

    public void syncGoodsToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncParanaItemToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncParanaShopCategoryToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncParanaShopCategoryBindingToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

}
