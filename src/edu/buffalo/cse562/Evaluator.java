package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Column col1 = (Column) left;
		Column col2 = (Column) right;
		int firstCol, secondCol;
		firstCol = getColumnID(col1);
		secondCol = getColumnID(col2);
		value = tuple[firstCol].Double() + tuple[secondCol].Double();
	}

	@Override
	public void visit(Division arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Column col1 = (Column) left;
		Column col2 = (Column) right;
		int firstCol, secondCol;
		firstCol = getColumnID(col1);
		secondCol = getColumnID(col2);
		value = tuple[firstCol].Double() / tuple[secondCol].Double();
	}

	@Override
	public void visit(Multiplication arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Column col1 = (Column) left;
		Column col2 = (Column) right;
		int firstCol, secondCol;
		firstCol = getColumnID(col1);
		secondCol = getColumnID(col2);
		value = tuple[firstCol].Double() * tuple[secondCol].Double();
	}

	@Override
	public void visit(Subtraction arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		Column col1 = (Column) left;
		Column col2 = (Column) right;
		int firstCol, secondCol;
		firstCol = getColumnID(col1);
		secondCol = getColumnID(col2);
		value = tuple[firstCol].Double() - tuple[secondCol].Double();
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (leftval && rightval) result = true;
		else result=false;
	}

	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		boolean leftval = this.getBool();
		
		arg0.getRightExpression().accept(this);
		boolean rightval = this.getBool();
		
		if (leftval || rightval) result = true;
		else result=false;
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo arg0) {		
	Column col1 = (Column)arg0.getLeftExpression();
	Expression right = arg0.getRightExpression();
	int firstCol = -1, secondCol = -1;
	for (int i=0; i<schema.length; i++) {
		if (schema[i].getColumnName().equals(col1.getColumnName())) {
			firstCol = i;
			break;
		}
	}
	if (right instanceof Column) {
		Column col2 = (Column) right;
		int i;
		for (i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col2.getColumnName())) {
				secondCol = i;
				break;
			}
		}
		if (schema[i].getColDataType().getDataType().equals("int")) {
			if(tuple[firstCol].Double() == tuple[secondCol].Double()){
				result = true;
				return;
			}
		}
		else {
			if ((tuple[firstCol].Date().equals(tuple[secondCol].Date()))) {
				result = true;
				return;
			}
		}

	} else {
		double rightVal = 0;
		
		if (right instanceof LongValue || right instanceof DoubleValue) {
			rightVal = Double.parseDouble(right.toString());

			if(tuple[firstCol].Double() == rightVal){
				result = true;
				return;
			}
		}
		else {
			try {
				StringValue date = (StringValue)right;	
				String dateVal = date.getValue();
				if ((tuple[firstCol].Date().equals((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal))))) {
					result = true;
					return;
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	result = false;	
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
	public void visit(GreaterThan arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		int firstCol = -1, secondCol = -1;
		if (!(left instanceof BinaryExpression)) {
			Column col1 = (Column) left;
			firstCol = getColumnID(col1);
			if (right instanceof Column) {
				Column col2 = (Column) right;
				secondCol = getColumnID(col2);
				if (schema[secondCol].getColDataType().getDataType().equals("int")) {
					if(tuple[firstCol].Double() > tuple[secondCol].Double()){
						result = true;
						return;
					}
				}
				else {
					if (tuple[firstCol].Date().after(tuple[secondCol].Date())) {
						result = true;
						return;
					}
				}
	
			} else {
				double rightVal = 0;
				
				if (right instanceof LongValue || right instanceof DoubleValue) {
					rightVal = Double.parseDouble(right.toString());
	
					if(tuple[firstCol].Double() > rightVal){
						result = true;
						return;
					}
				}
				else {
					try {
						StringValue date = (StringValue)right;	
						String dateVal = date.getValue();
						if (tuple[firstCol].Date().after((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal)))) {
							result = true;
							return;
						}
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			left.accept(this);

			double rightVal = Double.parseDouble(right.toString());
			if(this.getValue() > rightVal){
				result = true;
				return;
			}
		}
		result = false;
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		Column col1 = (Column)arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		int firstCol = -1, secondCol = -1;
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col1.getColumnName())) {
				firstCol = i;
				break;
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int i;
			for (i=0; i<schema.length; i++) {
				if (schema[i].getColumnName().equals(col2.getColumnName())) {
					secondCol = i;
					break;
				}
			}
			if (schema[i].getColDataType().getDataType().equals("int")) {
				if(tuple[firstCol].Double() >= tuple[secondCol].Double()){
					result = true;
					return;
				}
			}
			else {
				if (!(tuple[firstCol].Date().before(tuple[secondCol].Date()))) {
					result = true;
					return;
				}
			}

		} else {
			double rightVal = 0;
			
			if (right instanceof LongValue || right instanceof DoubleValue) {
				rightVal = Double.parseDouble(right.toString());

				if(tuple[firstCol].Double() >= rightVal){
					result = true;
					return;
				}
			}
			else {
				try {
					StringValue date = (StringValue)right;	
					String dateVal = date.getValue();
					if (!(tuple[firstCol].Date().before((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal))))) {
						result = true;
						return;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
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
		Column col1 = (Column)arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		int firstCol = -1, secondCol = -1;
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col1.getColumnName())) {
				firstCol = i;
				break;
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int i;
			for (i=0; i<schema.length; i++) {
				if (schema[i].getColumnName().equals(col2.getColumnName())) {
					secondCol = i;
					break;
				}
			}
			if (schema[i].getColDataType().getDataType().equals("int")) {
				if(tuple[firstCol].Double() < tuple[secondCol].Double()){
					result = true;
					return;
				}
			}
			else {
				if (tuple[firstCol].Date().before(tuple[secondCol].Date())) {
					result = true;
					return;
				}
			}

		} else {
			double rightVal = 0;
			
			if (right instanceof LongValue || right instanceof DoubleValue) {
				rightVal = Double.parseDouble(right.toString());

				if(tuple[firstCol].Double() < rightVal){
					result = true;
					return;
				}
			}
			else {
				try {
					StringValue date = (StringValue)right;	
					String dateVal = date.getValue();
					if (tuple[firstCol].Date().before((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal)))) {
						result = true;
						return;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		result = false;
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		Column col1 = (Column)arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		int firstCol = -1, secondCol = -1;
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col1.getColumnName())) {
				firstCol = i;
				break;
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int i;
			for (i=0; i<schema.length; i++) {
				if (schema[i].getColumnName().equals(col2.getColumnName())) {
					secondCol = i;
					break;
				}
			}
			if (schema[i].getColDataType().getDataType().equals("int")) {
				if(tuple[firstCol].Double() <= tuple[secondCol].Double()){
					result = true;
					return;
				}
			}
			else {
				if (!(tuple[firstCol].Date().after(tuple[secondCol].Date()))) {
					result = true;
					return;
				}
			}

		} else {
			double rightVal = 0;
			
			if (right instanceof LongValue || right instanceof DoubleValue) {
				rightVal = Double.parseDouble(right.toString());

				if(tuple[firstCol].Double() <= rightVal){
					result = true;
					return;
				}
			}
			else {
				try {
					StringValue date = (StringValue)right;	
					String dateVal = date.getValue();
					if (!(tuple[firstCol].Date().after((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal))))) {
						result = true;
						return;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		result = false;		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		Column col1 = (Column)arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		int firstCol = -1, secondCol = -1;
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col1.getColumnName())) {
				firstCol = i;
				break;
			}
		}
		if (right instanceof Column) {
			Column col2 = (Column) right;
			int i;
			for (i=0; i<schema.length; i++) {
				if (schema[i].getColumnName().equals(col2.getColumnName())) {
					secondCol = i;
					break;
				}
			}
			if (schema[i].getColDataType().getDataType().equals("int")) {
				if(tuple[firstCol].Double() != tuple[secondCol].Double()){
					result = true;
					return;
				}
			}
			else {
				if (!(tuple[firstCol].Date().equals(tuple[secondCol].Date()))) {
					result = true;
					return;
				}
			}

		} else {
			double rightVal = 0;
			
			if (right instanceof LongValue || right instanceof DoubleValue) {
				rightVal = Double.parseDouble(right.toString());

				if(tuple[firstCol].Double() != rightVal){
					result = true;
					return;
				}
			}
			else {
				try {
					StringValue date = (StringValue)right;	
					String dateVal = date.getValue();
					if (!(tuple[firstCol].Date().equals((new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH).parse(dateVal))))) {
						result = true;
						return;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
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
