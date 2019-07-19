package com.example.demo;

public class BootDemo {

	public BootDemo() {
		super();
		System.out.println("Object created");
	}


	private int id;
	private String name;
	private String address;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	

public void show() {
	System.out.println("In show");
}
}