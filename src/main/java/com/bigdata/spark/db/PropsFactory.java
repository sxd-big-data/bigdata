package com.bigdata.spark.db;

import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.*;

public class PropsFactory {
    public static MysqlProps createMysqlProps(String prefix) {
        try {
            Properties props = PropertiesLoaderUtils.loadAllProperties("application.properties");
            MysqlProps mysqlProps = new MysqlProps();
            mysqlProps.setDriver(props.getProperty(prefix + ".mysql.jdbc.driver"));
            mysqlProps.setUrl(props.getProperty(prefix + ".mysql.jdbc.url"));
            mysqlProps.setUsername(props.getProperty(prefix + ".mysql.jdbc.username"));
            mysqlProps.setPassword(props.getProperty(prefix + ".mysql.jdbc.password"));
            System.out.println("mysqlProps : " + mysqlProps);
            return mysqlProps;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> createMysqlPropsMap(String prefix) {
        try {
            Properties props = PropertiesLoaderUtils.loadAllProperties("application.properties");
            Map<String, String> mysqlPropsMap = new HashMap<>();
            Set<String> propNames = props.stringPropertyNames();
            if(null != propNames) {
                for(String name : propNames) {
                    if(name.startsWith(prefix)) {
                        mysqlPropsMap.put(Arrays.stream(name.split("\\.")).reduce((f, s) -> s).get(), props.getProperty(name));
                    }
                }
            }
            System.out.println("mysqlPropsMap : " + prefix + " = " + mysqlPropsMap);
            return mysqlPropsMap;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
