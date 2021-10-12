package com.bigdata.mysql.util;

import javax.sql.DataSource;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.util.StringUtils;

import com.bigdata.mysql.config.DynamicDataSource;
import com.bigdata.mysql.context.SpringContextHolder;
import com.bigdata.mysql.vo.DbInfo;

/**
 * 数据源工具
 *
 * @author: mason
 * @since: 2020/1/9
 **/
public class DataSourceUtil {
    /**
     * 创建新的数据源
     * @param dbInfo
     * @return
     */
    public static DataSource makeNewDataSource(DbInfo dbInfo){
        String url = "jdbc:mysql://"+dbInfo.getIp() + ":"+dbInfo.getPort()+"/"+dbInfo.getDbName()
                +"?useSSL=false&serverTimezone=GMT%2B8&characterEncoding=UTF-8";
        String driveClassName = StringUtils.isEmpty(dbInfo.getDriveClassName())? "com.mysql.cj.jdbc.Driver":dbInfo.getDriveClassName();
        return DataSourceBuilder.create().url(url)
                .driverClassName(driveClassName)
                .username(dbInfo.getUsername())
                .password(dbInfo.getPassword())
                .build();
    }

    /**
     * 添加数据源到动态源中
     * @param key
     * @param dataSource
     */
    public static void addDataSourceToDynamic(String key, DataSource dataSource){
        DynamicDataSource dynamicDataSource = SpringContextHolder.getContext().getBean(DynamicDataSource.class);
        dynamicDataSource.addDataSource(key,dataSource);
    }

    /**
     * 根据数据库连接信息添加数据源到动态源中
     * @param key
     * @param dbInfo
     */
    public static void addDataSourceToDynamic(String key, DbInfo dbInfo){
        DataSource dataSource = makeNewDataSource(dbInfo);
        addDataSourceToDynamic(key,dataSource);
    }
}
