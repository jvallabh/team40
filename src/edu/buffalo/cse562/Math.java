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
		if(schema[i].colDef.getColDataType().getDataType().equals("double"))
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
		if(schema[i].colDef.getColDataType().getDataType().equals("double"))
			return Double.toString(ans);
		else
			return Integer.toString((int)ans);
		
	}
	public static String getSum(String a , String b, int i, ColumnInfo[] schema){
		double c = Double.parseDouble(a);
		double d = Double.parseDouble(b);
		
		if(schema[i].colDef.getColDataType().getDataType().equals("double"))
			return Double.toString(c+d);
		else
			return Integer.toString((int)(c+d));	
		
			
	}
	public static Type getDataType( Column expr, ColumnInfo[] schema) {
		
		
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
			} else if (schema[colID].colDef.getColDataType().getDataType().equals("string")) {
				return Type.STRING;
			} else if (schema[colID].colDef.getColDataType().getDataType().equals("date")) {
				return Type.DATE;
			}
			return Type.DOUBLE;
			// Sets the data-type of the element	
		
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
	
	public static Type getDataType(Expression left, Expression right,ColumnInfo[] schema) {
		Expression expr;
		if (left instanceof Column) {
			expr = left;
		} else {
			expr = right;
		}
		if (right instanceof BinaryExpression) {
			if (left instanceof BinaryExpression) {
				return getNumType(((BinaryExpression)left).getLeftExpression(),schema);
			} else {
				expr = left;
			}
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
			} else if (schema[colID].colDef.getColDataType().getDataType().equals("string")) {
				return Type.STRING;
			} else if (schema[colID].colDef.getColDataType().getDataType().equals("date")) {
				return Type.DATE;
			}
			// Sets the data-type of the element
		} else if (expr instanceof LongValue) {
			return Type.INT;
		} else if (expr instanceof DoubleValue) {
			return Type.DOUBLE;
		} else if (expr instanceof StringValue) {
			return Type.STRING;
		} else if (expr instanceof DateValue) {
			return Type.DATE;
		}
		return Type.STRING;
	}

}
