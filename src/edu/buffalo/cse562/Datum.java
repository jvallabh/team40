package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Date;
import java.util.Locale;

public class Datum {
	String element;
	
	Datum (String element){
		this.element = element;
	}
	
	public double Double() {
		return Double.parseDouble(element);
	}
	
	public long Long() {
		return Long.parseLong(element);
	}
	
	public Integer Int() {
		return Integer.parseInt(element);
	}
	
	public String String() {
		return element;
	}

	public String toString() {
		return element;
	}
	
	public float Float() {
		return Float.parseFloat(element);
	}
	
	public Date Date() {	
		try {
			return new Date(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(element).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

}
