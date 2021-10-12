package com.bigdata.hive.services;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.druid.support.json.JSONUtils;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.bigdata.hive.hiveDao.OrderDeliverHiveRepository;
import com.bigdata.mysql.entity.OrderDeliverDetailData;
import com.bigdata.mysql.mapper.OrderDeliverDataMapper;
import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;

@Service
public class OrderDeliverService {
	@Autowired
	private OrderDeliverDataMapper orderDelMapper;
	
	@Autowired
	private OrderDeliverHiveRepository orderDeliverRep;
	public void insertALL() {
		/*
		 * PageHelper.startPage(1, 1000); QueryWrapper<OrderDeliverDetailData>
		 * queryWrapper = new QueryWrapper<>(); List<OrderDeliverDetailData>
		 * orderDeliverList = orderDelMapper.selectAll(queryWrapper);
		 * System.out.println("Total: " + ((Page) orderDeliverList).getTotal()); Long
		 * count =((Page) orderDeliverList).getTotal(); for(int
		 * i=0;i<Math.floor(count/1000+1);i++) { PageHelper.startPage(i+1, i*1000);
		 * orderDeliverList = orderDelMapper.selectAll(queryWrapper); for(int
		 * j=0;j<orderDeliverList.size();j++) { OrderDeliverDetailData d
		 * =orderDeliverList.get(j); System.out.println("orderId: " +
		 * d.getExternalSubOrderId()); orderDeliverRep.insert(orderDeliverList.get(j));
		 * } }
		 */
		
		doProcess();
	}
	
	private void doProcess(){
		PageHelper.startPage(1, 1000);
		QueryWrapper<OrderDeliverDetailData> queryWrapper = new QueryWrapper<>();
		List<OrderDeliverDetailData> orderDeliverList = orderDelMapper.selectAll(queryWrapper);
		Long count =((Page) orderDeliverList).getTotal();
		ExecutorService exec = null;
		try{
			for(int i=0;i<Math.floor(count/1000+1);i++) {
				PageHelper.startPage(i+1, 1000);
				exec = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
						Runtime.getRuntime().availableProcessors() * 2, 60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
				orderDeliverList = orderDelMapper.selectAll(queryWrapper);
				for(int j=0;j<orderDeliverList.size();j++) {
					HivePoolThreadRunnable thread = new HivePoolThreadRunnable(orderDeliverList.get(j));
					exec.execute(thread);
				}
				
				exec.shutdown();
				try {
					while(exec.awaitTermination(10, TimeUnit.SECONDS));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		System.out.println("thread finished");
	}	
	
	
	class HivePoolThreadRunnable implements Runnable{
		private OrderDeliverDetailData d;
		
		public HivePoolThreadRunnable(OrderDeliverDetailData d){
			this.d = d;
		}
		/**
		* <p>Title: run</p>  
		* <p>Description: </p>    
		* @see java.lang.Runnable#run()  
		*/  
		@Override
		public void run() {
//			System.out.println(JSONUtils.toJSONString(d));
			orderDeliverRep.insert(d);
		}
		
	} 
}
