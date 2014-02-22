package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Evaluator.Type;

public class Math {
	
	public static String getMin(String a , String b, int i, ColumnInfo[] schema){
		double c = Double.parseDouble(a);
		double d = Double.parseDouble(b);
		double ans;
		
		if(c>d)
			ans = d ;
		else
			ans = c;
		
		if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("double"))
			return Double.toString(ans);
		else if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("decimal"))
			return Double.toString(ans);
		else
			return Integer.toString((int)ans);	
	}
	
	public static String getMax(String a , String b, int i, ColumnInfo[] schema){
		double c = Double.parseDouble(a);
		double d = Double.parseDouble(b);
		double ans;
		
		if(c>d)
			ans = c ;
		else
			ans = d;
		if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("double"))
			return Double.toString(ans);
		else if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("decimal"))
			return Double.toString(ans);
		else
			return Integer.toString((int)ans);
		
	}
	
	public static String getSum(String a , String b, int i, ColumnInfo[] schema){
		double c = Double.parseDouble(a);
		double d = Double.parseDouble(b);
		
		if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("double"))
			return Double.toString(c+d);
		else if(schema[i].colDef.getColDataType().getDataType().equalsIgnoreCase("decimal"))
			return Double.toString(c+d);
		else
			return Integer.toString((int)c+(int)d);		
	}
	
	public static Type getNumType(Expression expr, ColumnInfo[] schema) {
		if (expr instanceof BinaryExpression) {
			return getNumType(((BinaryExpression) expr).getLeftExpression(),schema);
		}
		if (expr instanceof Column) {
			Column col = (Column) expr;
			int colID;
			// Gets the corresponding columnID from the schema
			for (colID=0; colID<schema.length; colID++) {
				if (schema[colID].colDef.getColumnName().equals(col.getColumnName())) {
					break;
				}
			}
			// Sets the data-type of the element in the column
			if (schema[colID].colDef.getColDataType().getDataType().equals("int")) {
				return Type.INT;
			} else if (schema[colID].colDef.getColDataType().getDataType().equals("double")) {
				return Type.DOUBLE;
			} 
			// Sets the data-type of the element
		} else if (expr instanceof LongValue) {
			return Type.INT;
		} else if (expr instanceof DoubleValue) {
			return Type.DOUBLE;
		}
		return Type.DOUBLE;
	}

}
