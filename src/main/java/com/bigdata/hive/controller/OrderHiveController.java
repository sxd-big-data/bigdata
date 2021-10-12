package com.bigdata.hive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.hive.owtDao.FulfillmentOrderRepository;
import com.bigdata.spark.service.SparkService;

@RestController
@RequestMapping("/fulOrder")
public class OrderHiveController {
	
	@Autowired
	private FulfillmentOrderRepository fulfillmentOrderRep;
	
	@Autowired
	private SparkService sparkService;
	
	@GetMapping("/createTable")
	public void createTable(){
		fulfillmentOrderRep.createTable();
	}
	@PostMapping("/loadOverData")
	public void loadOverData(@RequestParam String filePath) {
		fulfillmentOrderRep.loadOverData(filePath);
	}
	
	@PostMapping("/loadData")
	public void loadData(@RequestParam String filePath) {
		fulfillmentOrderRep.loadData(filePath);
	}
	
	@PostMapping("/loadTmpData")
	public void loadTmpData(@RequestParam String filePath) {
		fulfillmentOrderRep.loadTmpData(filePath);
	}
	
	@GetMapping("/deleteAll")
	public void deleteAll() {
		fulfillmentOrderRep.deleteAll();
	}
	
	@GetMapping("/updateHisData")
	public void updateHisData() {
		String[] sql =fulfillmentOrderRep.updateHisData();
		sparkService.updateHisData("fulfillmentOrderRep", sql[0]);
		sparkService.updateHisData("fulfillmentOrderRep", sql[1]);
	}

}
