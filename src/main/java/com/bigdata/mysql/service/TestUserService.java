package com.bigdata.mysql.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.bigdata.mysql.annotation.DS;
import com.bigdata.mysql.constants.DataSourceConstants;
import com.bigdata.mysql.entity.TestUser;
import com.bigdata.mysql.mapper.TestUserMapper;

/**
 * 服务类，数据源注解在方法
 *
 * @author: mason
 * @since: 2020/1/9
 **/
@Service
public class TestUserService {
    @Autowired
    private TestUserMapper testUserMapper;


    /**
     * 查询master库User
     * @return
     */
    @DS(DataSourceConstants.DS_KEY_MASTER)
    public List<TestUser> getMasterUser(){
        QueryWrapper<TestUser> queryWrapper = new QueryWrapper<>();
        List<TestUser> resultData = testUserMapper.selectAll(queryWrapper.isNotNull("name"));
        return resultData;
    }

    /**
     * 查询slave库User
     * @return
     */
    @DS(DataSourceConstants.DS_KEY_SLAVE)
    public List<TestUser> getSlaveUser(){
        return testUserMapper.selectList(null);
    }

}
