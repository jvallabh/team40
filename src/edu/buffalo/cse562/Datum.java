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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((element == null) ? 0 : element.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Datum other = (Datum) obj;
		if (element == null) {
			if (other.element != null)
				return false;
		} else if (!element.equals(other.element))
			return false;
		return true;
	}
	

}
