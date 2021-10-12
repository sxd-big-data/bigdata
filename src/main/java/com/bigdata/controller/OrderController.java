package com.bigdata.controller;

import io.swagger.annotations.ApiOperation;
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
public class OrderController {
	@Autowired
	private OrderService orderService;
	
//	@ApiImplicitParams({
//		  @ApiImplicitParam(name="overWrite",value="是否覆盖源数据",dataType="boolean", paramType = "query",example="false"),
//		  @ApiImplicitParam(name="tableName",value="表名",dataType="string", paramType = "query",example="fulfillment_order")})
//	@GetMapping("/syncOrderToHdfs")
	public void syncOrderToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
//		orderService.syncOrderToHdfs(false, "fulfillment_order");

	}

	@ApiOperation(value = "订单",  notes = "字符串")
	@GetMapping("/syncOrderByNameToHdfs")
	public void syncOrderByNameToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncOrderByNameToHdfs(false, "order");

	}

	@ApiOperation(value = "订单行",  notes = "字符串")
	@GetMapping("/syncOrderLineToHdfs")
	public void syncOrderLineToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncOrderLineToHdfs(false, "order_line");

	}

	@ApiOperation(value = "购物单",  notes = "字符串")
	@GetMapping("/syncPurchaseOrderToHdfs")
	public void syncPurchaseOrderToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncPurchaseOrderToHdfs(false, "purchase_order");

	}

	@ApiOperation(value = "订单和优惠劵关系",  notes = "字符串")
	@GetMapping("/syncOrderSideEffectToHdfs")
	public void syncOrderSideEffectToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncOrderSideEffectToHdfs(false, "order_side_effect");

	}

	@ApiOperation(value = "员工折扣明细表",  notes = "字符串")
	@GetMapping("/syncEmployOrderSideEffectToHdfs")
	public void syncEmployOrderSideEffectToHdfs() {//@ApiParam(name = "overWrite", value = "是否覆盖源数据",defaultValue = "false") Boolean overWrite,@ApiParam(name = "tableName", value = "表名",defaultValue = "fulfillment_order") String tableName
		// TODO Auto-generated method stub
		orderService.syncEmployOrderSideEffectToHdfs(false, "employ_order_side_effect");

	}

}
