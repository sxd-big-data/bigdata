package com.hive;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		Calendar nowDate = Calendar.getInstance();
		Date nowTime = new Date();
		nowDate.setTime(nowTime);
		nowDate.add(Calendar.MONTH, -1);
		nowDate.set(Calendar.DAY_OF_MONTH, 1);
//		nowDate.set(Calendar.DATE, -1);
		String startDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate.getTime());
		// nowDate = Calendar.getInstance();
		nowDate.add(Calendar.MONTH, +1);
		nowDate.set(Calendar.DAY_OF_MONTH, +1);
		String endDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate.getTime());
		System.out.println("startDate:::" + startDate + ",endDate::::" + endDate);
		System.out.println("Hello World!");
	}
}
