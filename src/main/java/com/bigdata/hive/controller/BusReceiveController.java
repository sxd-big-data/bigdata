package com.bigdata.hive.controller;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.hive.entity.BusReceiverEntity;
import com.bigdata.hive.hiveDao.OrderDeliverHiveRepository;
import com.bigdata.hive.services.BusReceiverSerivce;
import com.bigdata.hive.services.OrderDeliverService;

@RestController
@RequestMapping("/bus")
public class BusReceiveController {
	
	@Resource
	private BusReceiverSerivce busReceiverService;
	
	@GetMapping("/createTable")
	public void createTable(){
		busReceiverService.createTable();
	}
	@PostMapping("/loadData")
	public void loadData(@RequestParam String filePath) {
		busReceiverService.loadData(filePath);
	}
	@PostMapping("/insert")
	public void insert(@RequestBody BusReceiverEntity busReceiver) {
		busReceiverService.insert(busReceiver);
	}
	@Autowired
	private OrderDeliverHiveRepository orderDeliverRep;
	@GetMapping("/deleteAll")
	public void deleteAll() {
		busReceiverService.deleteAll();
		orderDeliverRep.deleteAll();
	}
	
	@PostMapping("/loadOrderData")
	public void loadOrderData(@RequestParam String filePath) {
		orderDeliverRep.loadData(filePath);
	}
	
	@Autowired
	private OrderDeliverService orderDeliverService;
	
	@GetMapping("/insertAll")
	public void insert() {
		orderDeliverService.insertALL();
	}
}
