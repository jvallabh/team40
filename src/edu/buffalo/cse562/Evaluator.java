package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator implements ExpressionVisitor {

	ColumnInfo[] schema;
	Datum[] tuple;
	boolean result;
	String value;
	static List distinctList = new ArrayList<>();
	HashMap<String, Integer> colHash; 
	
	public Evaluator(ColumnInfo[] schema, Datum[] tuple) {
		this.schema = schema;
		this.tuple = tuple;
	}
	
	public Evaluator(ColumnInfo[] schema) {
		this.schema = schema;
		this.colHash = createHash();
	}
	
	public HashMap createHash() {
		HashMap colHash = new HashMap<>();
		for (int i=0; i<schema.length; i++) {
			colHash.put(schema[i].colDef.getColumnName(), i);
			colHash.put(schema[i].tableName + schema[i].colDef.getColumnName(), i);
		}
		return colHash;
	}
	
	public void sendTuple (Datum[] tuple) {
		this.tuple = tuple;
	}
	
	public enum Type {
		INT, STRING, DOUBLE, DATE
	}
	
	public boolean getBool() {
		return result;
	}
	
	public String getValue() {
		return value;
	}
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		if(arg0 instanceof Function) {
			Function aggFunc = (Function) arg0;
			if(arg0.getName().equalsIgnoreCase("COUNT")){
				value = "1";
				if(arg0.isDistinct()) {
					ExpressionList expList = arg0.getParameters();
					List<Expression> exps = expList.getExpressions();
					StringBuilder sb= new StringBuilder();
					Column col=null;
					for(Expression e:exps){
						col = (Column) e;
						sb.append(tuple[getColumnID(col)]);
					}
					if(distinctList.contains(sb.toString()))
						value="0";
					else
						distinctList.add(sb.toString());
					sb.delete(0, sb.length());
				}
				return;
			}
			ExpressionList expList = aggFunc.getParameters();
			Column expCol = null;
			for (Iterator iter = expList.getExpressions().iterator(); iter.hasNext();) {
				 ((Expression)iter.next()).accept(this);
				 break;
			}
			
			return;	
		}
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		value = arg0.toString();
	}

	@Override
	public void visit(LongValue arg0) {
		value = arg0.toString();
	}

	@Override
	public void visit(DateValue arg0) {
		value = arg0.toString();		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		value = arg0.getValue();
		
	}

	public Type getNumType(Expression expr) {
		if (expr instanceof BinaryExpression) {
			return getNumType(((BinaryExpression) expr).getLeftExpression());
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
			if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("int")) {
				return Type.INT;
			} else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("double")) {
				return Type.DOUBLE;
			} else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
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
	
	public Type getDataType(Expression left, Expression right) {
		Expression expr;
		if (left instanceof Column) {
			expr = left;
		} else {
			expr = right;
		}
		if (right instanceof BinaryExpression) {
			if (left instanceof BinaryExpression) {
				return getNumType(((BinaryExpression)left).getLeftExpression());
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
			if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("int")) {
				return Type.INT;
			} else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("double")) {
				return Type.DOUBLE;
			} else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("decimal")) {
				return Type.DOUBLE;
			}else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("string")) {
				return Type.STRING;
			} else if (schema[colID].colDef.getColDataType().getDataType().equalsIgnoreCase("date")) {
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
	
	@Override
	public void visit(Addition arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataTypeLeft = getNumType(left);
		Type dataTypeRight = getNumType(right);
		String leftVal, rightVal;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		if (dataTypeLeft == Type.DOUBLE || dataTypeRight == Type.DOUBLE) {
			Double val = (Double)((Double.parseDouble(leftVal)*1000.0) + (Double.parseDouble(rightVal))*1000.0)/1000.0;
			value = val.toString();
		}
		else
			value = ((Integer)(Integer.parseInt(leftVal) + Integer.parseInt(rightVal))).toString();
	}

	@Override
	public void visit(Division arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				value = ((Integer)(Integer.parseInt(leftVal)/Integer.parseInt(rightVal))).toString();
				break;
			case DOUBLE:
				Double val = (Double)((Double.parseDouble(leftVal)*1000.0) - (Double.parseDouble(rightVal))*1000.0);
				value = val.toString();
				break;
		}
	}

	@Override
	public void visit(Multiplication arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				value = ((Integer)(Integer.parseInt(leftVal) * Integer.parseInt(rightVal))).toString();
				break;
			case DOUBLE:
				Double val = (Double)((Double.parseDouble(leftVal)*1000.0) * (Double.parseDouble(rightVal))*1000.0)/1000000.0;
				value = val.toString();
				break;
		}
	}

	@Override
	public void visit(Subtraction arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataTypeLeft = getNumType(left);
		Type dataTypeRight = getNumType(right);
		String leftVal, rightVal;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		if(dataTypeLeft==Type.DOUBLE||dataTypeRight==Type.DOUBLE) {
			Double val = (Double)((Double.parseDouble(leftVal)*1000.0) - (Double.parseDouble(rightVal))*1000.0)/1000.0;
			value = val.toString();
		}
		else
			value = ((Integer)(Integer.parseInt(leftVal) - Integer.parseInt(rightVal))).toString();
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		if (leftval == false) { 
			result = false;
			return;
		}
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (rightval) 
			result = true;
		else 
			result=false;
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		
		if (leftval == true) {
			result = true;
			return;
		}
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (rightval) 
			result = true;
		else 
			result=false;
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	public Date getDateVal(String date) {
		try {
			return new Date(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(date).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	public int getColumnID(Column col) {
		if (col.getTable().getName() == null) {
			return colHash.get(col.getColumnName());
			/*for (int i=0; i<schema.length; i++) {
				if (schema[i].colDef.getColumnName().equals(col.getColumnName())) {
					return i;
				}
			}*/
		} else {
			return colHash.get(col.getTable()+ col.getColumnName());
			/*for (int i=0; i<schema.length; i++) {
				if ((schema[i].colDef.getColumnName().equals(col.getColumnName())) && (schema[i].tableName.equals(col.getTable().getName()))) {
					return i;
				}
			}*/	
		}
		//return -1;
	}
	
	@Override
	public void visit(EqualsTo arg0) {	
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) == Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) == Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				if (leftVal.equals(rightVal)) {
					result = true;
					return;
				}
				break;
			case DATE:
				if (getDateVal(leftVal).equals(getDateVal(rightVal))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}
	
	@Override
	public void visit(GreaterThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) > Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) > Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				break;
			case DATE:
				if (getDateVal(leftVal).after(getDateVal(rightVal))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) >= Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) >= Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				break;
			case DATE:
				if (!(getDateVal(leftVal).before(getDateVal(rightVal)))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) < Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) < Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				break;
			case DATE:
				if (getDateVal(leftVal).before(getDateVal(rightVal))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}
	
	@Override
	public void visit(MinorThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) <= Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) <= Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				break;
			case DATE:
				if (!(getDateVal(leftVal).after(getDateVal(rightVal)))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}
	
	@Override
	public void visit(NotEqualsTo arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		left.accept(this);
		leftVal = this.getValue();
		right.accept(this);
		rightVal = this.getValue();
		switch (dataType) {
			case INT:
				if (Integer.parseInt(leftVal) != Integer.parseInt(rightVal)) {
					result = true;
					return;
				}
				break;
			case DOUBLE:
				if (Double.parseDouble(leftVal) != Double.parseDouble(rightVal)) {
					result = true;
					return;
				}
				break;
			case STRING:
				if (!(leftVal.equals(rightVal))) {
					result = true;
					return;
				}
				break;
			case DATE:
				if (!(getDateVal(leftVal).equals(getDateVal(rightVal)))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}
	
	@Override
	public void visit(Column arg0) {
		Column col = (Column) arg0;
		int colID = getColumnID(col);
		value = tuple[colID].String();
	}
	
	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}
}