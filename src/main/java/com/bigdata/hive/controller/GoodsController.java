package com.bigdata.hive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.hive.owtDao.FulfillmentGoodsRepository;
import com.bigdata.hive.owtDao.FulfillmentOrderRepository;
import com.bigdata.spark.service.SparkService;

@RestController
@RequestMapping("/fulGoods")
public class GoodsController {
	
	@Autowired
	private FulfillmentGoodsRepository fulfillmentGoodsRep;
	@Autowired
	private SparkService sparkService;
	
	@GetMapping("/createTable")
	public void createTable(){
		fulfillmentGoodsRep.createTable();
	}
	
	@PostMapping("/loadOverData")
	public void loadOverData(@RequestParam String filePath) {
		fulfillmentGoodsRep.loadOverData(filePath);
	}
	
	@PostMapping("/loadTmpData")
	public void loadTmpData(@RequestParam String filePath) {
		fulfillmentGoodsRep.loadTmpData(filePath);
	}
	
	@GetMapping("/updateHisData")
	public void updateHisData() {
		String[] sql = fulfillmentGoodsRep.updateHisData();
		sparkService.updateHisData("fulfillmentGoodsRep", sql[0]);
		sparkService.updateHisData("fulfillmentGoodsRep", sql[1]);
	}
	
	@PostMapping("/loadData")
	public void loadData(@RequestParam String filePath) {
		fulfillmentGoodsRep.loadData(filePath);
	}
	
	@GetMapping("/deleteAll")
	public void deleteAll() {
		fulfillmentGoodsRep.deleteAll();
	}
	
	
	

}
