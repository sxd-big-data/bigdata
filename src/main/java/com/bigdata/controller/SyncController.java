package com.bigdata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.service.OrderService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping("/bigdata")
public class SyncController {
    //以订单为例
	@Autowired
	private OrderService orderService;
	
	//样例:同步mysql数据到hdfs,如果overWrite=true,则直接清空当前表数据，重新写入，否则追加数据到原有数据后面
	@ApiImplicitParams({
		  @ApiImplicitParam(name="overWrite",value="是否覆盖源数据",dataType="boolean", paramType = "query",example="false"),
		  @ApiImplicitParam(name="tableName",value="表名",dataType="string", paramType = "query",example="order")})
	@GetMapping("/syncOrderToHdfs")
	public void syncOrderToHdfs(@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName ) {//
		// TODO Auto-generated method stub
		orderService.syncMysqlToHdfs(overWrite, tableName);

	}

}
