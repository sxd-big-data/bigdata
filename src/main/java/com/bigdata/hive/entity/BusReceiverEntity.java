package com.bigdata.hive.entity;

public class BusReceiverEntity {
	private Long id;
	private String name;
	private String address;
	private String enName;
	private Integer memberFamily;
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	
	public String getEnName() {
		return enName;
	}
	public void setEnName(String enName) {
		this.enName = enName;
	}
	public Integer getMemberFamily() {
		return memberFamily;
	}
	public void setMemberFamily(Integer memberFamily) {
		this.memberFamily = memberFamily;
	}
	
}
