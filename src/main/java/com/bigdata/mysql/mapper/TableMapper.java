package com.bigdata.mysql.mapper;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bigdata.mysql.entity.TestUser;

/**
 * 表处理mapper
 *
 * @author: mason
 * @since: 2020/1/9
 **/
@Repository
public interface TableMapper extends BaseMapper<TestUser> {
    /**
     * 查询表信息
     * @return
     */
    @Select("select table_name, table_comment, create_time, update_time " +
            " from information_schema.tables " +
            " where table_schema = (select database())")
    List<Map<String,Object>> selectTableList();
}
