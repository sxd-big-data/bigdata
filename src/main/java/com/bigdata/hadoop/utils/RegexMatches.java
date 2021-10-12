package com.bigdata.hadoop.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMatches {
	private static final String pattern=".txt$";
	public static Boolean accept(String str) {
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(str);
		System.out.println(m.find());
		return m.find();
	}
	
	public static void main(String args[]) {
		String str = "test.txt";
		String pattern = ".txt$";

		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(str);
		System.out.println(m.find());
	}
}
