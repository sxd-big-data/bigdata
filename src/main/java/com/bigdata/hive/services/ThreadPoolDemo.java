package com.bigdata.hive.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
* @ClassName: ThreadPoolDemo  
* @Description: TODO(用来做线程池的demo)  
* @author proven  
* @date 2019年4月3日  
*
 */
public class ThreadPoolDemo {
	/*
	 * public static void main(String[] args) { ThreadPoolDemo demo = new
	 * ThreadPoolDemo(); demo.doProcess();
	 * 
	 * }
	 */
	
	
	private void doProcess(){
		ExecutorService exec = null;
		try{
			exec = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
					Runtime.getRuntime().availableProcessors() * 2, 60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
			for(int i=0;i<10;i++){
				TestPoolThreadRunnable thread = new TestPoolThreadRunnable(i);
				exec.execute(thread);
			}
			exec.shutdown();
			try {
				while(exec.awaitTermination(10, TimeUnit.SECONDS));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		System.out.println("thread finished");
	}
	
	
	
	
	class TestPoolThreadRunnable implements Runnable{
		private Integer count;
		
		public TestPoolThreadRunnable(Integer count){
			this.count = count;
		}
		/**
		* <p>Title: run</p>  
		* <p>Description: </p>    
		* @see java.lang.Runnable#run()  
		*/  
		@Override
		public void run() {
			System.out.println(count++);
			
		}
		
	}
	
}