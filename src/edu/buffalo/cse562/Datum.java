package edu.buffalo.cse562;

public class Datum {
	String element;
	
	Datum (String element){
		this.element = element;
	}
	
	public long Long() {
		return Long.parseLong(element);
	}
	
	public Integer Int() {
		return Integer.parseInt(element);
	}
	
	public String Str() {
		return element;
	}
	
	public float Float() {
		return Float.parseFloat(element);
	}
	

}
