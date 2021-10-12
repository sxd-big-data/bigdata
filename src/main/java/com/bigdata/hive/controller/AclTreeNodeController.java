package com.bigdata.hive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bigdata.hive.owtDao.AclTreeNodeRepository;
import com.bigdata.hive.owtDao.MilkSationRepository;

@RestController
@RequestMapping("/aclTree")
public class AclTreeNodeController {
	
	@Autowired
	private AclTreeNodeRepository aclTreeNodeRep;
	
	@GetMapping("/createTable")
	public void createTable(){
		aclTreeNodeRep.createTable();
	}
	@PostMapping("/loadData")
	public void loadData(@RequestParam String filePath) {
		aclTreeNodeRep.loadOverData(filePath);
	}
	
	@GetMapping("/deleteAll")
	public void deleteAll() {
		aclTreeNodeRep.deleteAll();
	}

}
