package com.bigdata.service;

import com.bigdata.hadoop.utils.HDFSPropertiesUtils;
import com.bigdata.util.BigDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;



@Service
public class UserService {
    private String hdfsDirectory= HDFSPropertiesUtils.getProperty("hdfs.base.directory");
    @Autowired
    private BigDataUtil bigDataUtil;

    public void syncUcUserToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);
    }


    public void syncUcUserDetailToHdfs(String tableName){
        bigDataUtil.syncTableToHdfs(hdfsDirectory,tableName);
    }

    public void syncUserBenefitToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);

    }

    public void syncUserBudgetToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);
    }

    public void syncUcUserThirdAccountToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);
    }

    public void syncShippingAddressToHdfs(Boolean overWrite, String tableName){
        bigDataUtil.syncTableHisToHdfs(overWrite,hdfsDirectory,tableName);

    }

}
