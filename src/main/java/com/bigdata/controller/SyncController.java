package com.bigdata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.service.AclTreeNodeService;
import com.bigdata.service.GoodsService;
import com.bigdata.service.MilkSationService;
import com.bigdata.service.OrderService;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping("/bigdata")
public class SyncController {
	@Autowired
	private OrderService orderService;
	
	@Autowired
	private GoodsService goodsService;
	
	@Autowired
	private AclTreeNodeService treeNodeService;
	
	@Autowired
	private MilkSationService milkSationService;
	
//	@ApiImplicitParams({
//		  @ApiImplicitParam(name="overWrite",value="是否覆盖源数据",dataType="boolean", paramType = "query",example="false"),
//		  @ApiImplicitParam(name="tableName",value="表名",dataType="string", paramType = "query",example="fulfillment_order")})
	@GetMapping("/syncOrderToHdfs")
	public void syncOrderToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncMysqlToHdfs(true, "fulfillment_order");

	}
	
	
	
	@GetMapping("/syncGoodsToHdfs")
	public void syncGoodsToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		goodsService.syncMysqlToHdfs(true, "fulfillment_goods");

	}
	
	@GetMapping(value = "/syncBaseDataToHdfs",name = "同步基础数据")
	public void syncBaseDataToHdfs() {
		treeNodeService.syncMysqlToHdfs(true, "acl_tree_node");
		milkSationService.syncMysqlToHdfs(true, "milk_station");
	}
	
}
