package com.bigdata.hive.services;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.bigdata.hive.entity.BusReceiverEntity;
import com.bigdata.hive.hiveDao.BusReceiverHiveRepository;
import com.bigdata.mysql.mapper.OrderDeliverDataMapper;

@Service
public class BusReceiverSerivce {
	
	@Resource
	private BusReceiverHiveRepository busReceiverRep;
	
	
	
	public void createTable(){
		busReceiverRep.createTable();
	}
	
	public void loadData(String filePath) {
		busReceiverRep.loadData(filePath);
	}
	
	public void insert(BusReceiverEntity busReceiver) {
		
		busReceiverRep.insert(busReceiver);
	}
	
	public void deleteAll() {
		busReceiverRep.deleteAll();
	}

}
