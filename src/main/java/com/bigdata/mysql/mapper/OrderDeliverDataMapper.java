package com.bigdata.mysql.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.toolkit.Constants;
import com.bigdata.mysql.entity.OrderDeliverDetailData;
import com.bigdata.mysql.entity.TestUser;

/**
 * 用户 Mapper
 *
 * @author mason
 * @date 2020-01-08
 */
@Repository
public interface OrderDeliverDataMapper extends BaseMapper<OrderDeliverDetailData> {

    /**
     * 自定义查询
     * @param wrapper 条件构造器
     * @return
     */
    List<OrderDeliverDetailData> selectAll(@Param(Constants.WRAPPER) Wrapper<OrderDeliverDetailData> wrapper);

}
