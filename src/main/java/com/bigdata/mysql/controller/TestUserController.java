package com.bigdata.mysql.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.bigdata.mysql.constants.DataSourceConstants;
import com.bigdata.mysql.entity.TestUser;
import com.bigdata.mysql.mapper.TableMapper;
import com.bigdata.mysql.mapper.TestUserMapper;
import com.bigdata.mysql.proxy.JdkParamDsMethodProxy;
import com.bigdata.mysql.service.TestUserService;
import com.bigdata.mysql.vo.DbInfo;
import com.bigdata.mysql.vo.ResponseResult;

/**
 * 用户 Controller
 *
 * @author mason
 * @date 2020-01-08
 */
@RestController
@RequestMapping("/user")
public class TestUserController {

    @Autowired
    private TestUserMapper testUserMapper;

    @Autowired
    private TestUserService testUserService;

    @Autowired
    private TableMapper tableMapper;

    /**
     * 根据数据库连接信息获取表信息
     * @param dbInfo，包括ip,port,dbName,driveClassName,username,password
     * @return
     */
    @GetMapping("table")
    public Object findWithDbInfo(DbInfo dbInfo) throws Exception {

        //数据源key
        String newDsKey = System.currentTimeMillis()+"";
//        //添加数据源
//        DataSourceUtil.addDataSourceToDynamic(newDsKey,dbInfo);
//        DynamicDataSourceContextHolder.setContextKey(newDsKey);
//        //查询表信息
//        List<Map<String, Object>> tables = tableMapper.selectTableList();
//        DynamicDataSourceContextHolder.removeContextKey();

        //使用代理切换数据源
        TableMapper tableMapperProxy = (TableMapper)JdkParamDsMethodProxy.createProxyInstance(tableMapper, newDsKey, dbInfo);
        List<Map<String, Object>> tables = tableMapperProxy.selectTableList();
        return ResponseResult.success(tables);
    }

    /**
     * 查询
     */
    @GetMapping("/find")
    public Object find(int id) {
        TestUser testUser = testUserMapper.selectOne(new QueryWrapper<TestUser>().eq("id" , id));
        if (testUser != null) {
            return ResponseResult.success(testUser);
        } else {
            return ResponseResult.error("没有找到该对象");
        }
    }

    /**
     * 查询全部
     */
    @GetMapping("/listall")
    public Object listAll() {
        int initSize = 2;
        Map<String, Object> result = new HashMap<>(initSize);
        //默认master数据源查询
        List<TestUser> masterUser = testUserService.getMasterUser();
        result.put(DataSourceConstants.DS_KEY_MASTER, masterUser);
        //从slave数据源查询
        List<TestUser> slaveUser = testUserService.getSlaveUser();
        result.put(DataSourceConstants.DS_KEY_SLAVE, slaveUser);
        //返回数据
        return ResponseResult.success(result);
    }

}
