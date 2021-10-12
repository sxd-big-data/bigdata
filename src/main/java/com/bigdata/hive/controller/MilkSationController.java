package com.bigdata.hive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.hive.owtDao.MilkSationRepository;

@RestController
@RequestMapping("/milkSation")
public class MilkSationController {
	
	@Autowired
	private MilkSationRepository milkSationRep;
	
	@GetMapping("/createTable")
	public void createTable(){
		milkSationRep.createTable();
	}
	@PostMapping("/loadData")
	public void loadData(@RequestParam String filePath) {
		milkSationRep.loadOverData(filePath);
	}
	
	
	@GetMapping("/deleteAll")
	public void deleteAll() {
		milkSationRep.deleteAll();
	}

}
