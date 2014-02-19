package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Date;
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
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator implements ExpressionVisitor {

	ColumnDefinition[] schema;
	Datum[] tuple;
	boolean result;
	double value;
	
	public Evaluator(ColumnDefinition[] schema, Datum[] tuple) {
		this.schema = schema;
		this.tuple = tuple;
	}
	
	public enum Type {
		INT, STRING, DOUBLE, DATE
	}
	
	public boolean getBool() {
		return result;
	}
	
	public double getValue() {
		return value;
	}
	
	public int getIntValue() {
		return (int)value;
	}
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
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
		//TODO: have to correctly implement this
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	public Type getNumType(Expression expr) {
		if (expr instanceof Column) {
			Column col = (Column) expr;
			int colID;
			// Gets the corresponding columnID from the schema
			for (colID=0; colID<schema.length; colID++) {
				if (schema[colID].getColumnName().equals(col.getColumnName())) {
					break;
				}
			}
			// Sets the data-type of the element in the column
			if (schema[colID].getColDataType().getDataType().equals("int")) {
				return Type.INT;
			} else if (schema[colID].getColDataType().getDataType().equals("double")) {
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
		if (expr instanceof Column) {
			Column col = (Column) expr;
			int colID;
			// Gets the corresponding columnID from the schema
			for (colID=0; colID<schema.length; colID++) {
				if (schema[colID].getColumnName().equals(col.getColumnName())) {
					break;
				}
			}
			// Sets the data-type of the element in the column
			if (schema[colID].getColDataType().getDataType().equals("int")) {
				return Type.INT;
			} else if (schema[colID].getColDataType().getDataType().equals("double")) {
				return Type.DOUBLE;
			} else if (schema[colID].getColDataType().getDataType().equals("string")) {
				return Type.STRING;
			} else if (schema[colID].getColDataType().getDataType().equals("date")) {
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
	
	//TODO Have to handle if either the right or left is a Binary Expression instead of just a value
	@Override
	public void visit(Addition arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			leftVal = tuple[leftCol].String();
		} else {
			leftVal = left.toString();
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			rightVal = tuple[rightCol].String();
		} else {
			rightVal = right.toString();
		}
		switch (dataType) {
			case INT:
				value = Integer.parseInt(leftVal) + Integer.parseInt(rightVal);
				break;
			case DOUBLE:
				value = Double.parseDouble(leftVal) + Double.parseDouble(rightVal);
				break;
		}
	}

	@Override
	public void visit(Division arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			leftVal = tuple[leftCol].String();
		} else {
			leftVal = left.toString();
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			rightVal = tuple[rightCol].String();
		} else {
			rightVal = right.toString();
		}
		switch (dataType) {
			case INT:
				value = Integer.parseInt(leftVal)/Integer.parseInt(rightVal);
				break;
			case DOUBLE:
				value = Double.parseDouble(leftVal)/Double.parseDouble(rightVal);
				break;
		}
	}

	@Override
	public void visit(Multiplication arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			leftVal = tuple[leftCol].String();
		} else {
			leftVal = left.toString();
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			rightVal = tuple[rightCol].String();
		} else {
			rightVal = right.toString();
		}
		switch (dataType) {
			case INT:
				value = Integer.parseInt(leftVal)*Integer.parseInt(rightVal);
				break;
			case DOUBLE:
				value = Double.parseDouble(leftVal)*Double.parseDouble(rightVal);
				break;
		}
	}

	@Override
	public void visit(Subtraction arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getNumType(left);
		String leftVal, rightVal;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			leftVal = tuple[leftCol].String();
		} else {
			leftVal = left.toString();
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			rightVal = tuple[rightCol].String();
		} else {
			rightVal = right.toString();
		}
		switch (dataType) {
			case INT:
				value = Integer.parseInt(leftVal) - Integer.parseInt(rightVal);
				break;
			case DOUBLE:
				value = Double.parseDouble(leftVal) - Double.parseDouble(rightVal);
				break;
		}
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (leftval && rightval) 
			result = true;
		else 
			result=false;
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (leftval || rightval) 
			result = true;
		else 
			result=false;
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	public Date getDateVal(Expression expr) {
		try {
			StringValue dateVal = (StringValue)expr;	
			String date = dateVal.getValue();
			return new Date(new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(date).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}	
	}

	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public void visit(EqualsTo arg0) {	
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Type dataType = getDataType(left, right);
		String leftVal = null, rightVal = null;
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else if (dataType == Type.STRING){
				leftVal = ((StringValue)left).getValue();
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else if (dataType == Type.STRING){
				rightVal = ((StringValue)right).getValue();
			} else {
				rightVal = right.toString();
			}
		}
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
				if (leftDateVal.equals(rightDateVal)) {
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
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else {
				rightVal = right.toString();
			}
		}
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
				if (leftDateVal.after(rightDateVal)) {
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
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else {
				rightVal = right.toString();
			}
		}
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
				if (!(leftDateVal.before(rightDateVal))) {
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
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else {
				rightVal = right.toString();
			}
		}
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
				if (leftDateVal.before(rightDateVal)) {
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
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else {
				rightVal = right.toString();
			}
		}
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
				if (!(leftDateVal.after(rightDateVal))) {
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
		Date leftDateVal = null, rightDateVal = null;
		if (left instanceof Column) {
			Column col1 = (Column) left;
			int leftCol = getColumnID(col1);
			if (dataType == Type.DATE) {
				leftDateVal = tuple[leftCol].Date();
			} else {
				leftVal = tuple[leftCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				leftDateVal = getDateVal(left);
			} else if (dataType == Type.STRING){
				leftVal = ((StringValue)left).getValue();
			} else {
				leftVal = left.toString();
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int rightCol = getColumnID(col2);
			if (dataType == Type.DATE) {
				rightDateVal = tuple[rightCol].Date();
			} else {
				rightVal = tuple[rightCol].String();
			}
		} else {
			if (dataType == Type.DATE) {
				rightDateVal = getDateVal(right);
			} else if (dataType == Type.STRING){
				rightVal = ((StringValue)right).getValue();
			} else {
				rightVal = right.toString();
			}
		}
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
				if (!(leftDateVal.equals(rightDateVal))) {
					result = true;
					return;
				}
				break;
		}
		result = false;
	}
	
	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		
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
