/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse562.Evaluator.Type;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class ProjectionOperator implements Operator {
	Operator input;
	ColumnInfo[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	ColumnInfo[] finalSchema;
	
	public ProjectionOperator(Operator input, ColumnInfo[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		this.finalSchema = changeSchema(selectItems);
	}
	
	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].colDef.getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	
	public ColDataType getNumType(Expression expr) {
		if (expr instanceof Parenthesis) {
			return getNumType(((Parenthesis) expr).getExpression());
		}
		if (expr instanceof BinaryExpression) {
			ColDataType colDataType = getNumType(((BinaryExpression) expr).getLeftExpression());
			if (colDataType == null) {
				return getNumType(((BinaryExpression) expr).getRightExpression());
			}
			return colDataType;	
		}
		else if (expr instanceof Column) {
			Column col = (Column) expr;
			return (schema[getColumnID(col)].colDef.getColDataType());
		}
		return null;
	}
	
	public String getTableName(Expression expr) {
		if (expr instanceof Column) {
			return ((Column) expr).getTable().getName();
		} else 
			return new String("dummy");
	}
	
	public ColumnInfo[] changeSchema(List selectItems) {
		if (selectItems.get(0) instanceof AllColumns)
		return schema;
		ColumnInfo[] schema = new ColumnInfo[selectItems.size()];
		SelectExpressionItem exp;
		for (int j=0; j<selectItems.size(); j++) {
			ColumnDefinition colDef = new ColumnDefinition();
			exp = (SelectExpressionItem)selectItems.get(j);
			if(exp.getAlias()!=null) {
				colDef.setColumnName(exp.getAlias());
			}
			else {
				colDef.setColumnName(exp.toString());
			}
			colDef.setColDataType(getNumType(exp.getExpression()));
			schema[j] = new ColumnInfo(colDef, getTableName(exp.getExpression()));
		}
		
		return schema;
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple = input.readOneTuple();
		ArrayList<Datum> tupleList = new ArrayList<>();
		Datum[] newTuple = null;
		if(tuple == null)
			return null;
		if (selectItems.get(0) instanceof AllColumns)
			return tuple;
		else {			
			SelectExpressionItem selectExp;
			Expression exp = null;
			for (int j=0; j<selectItems.size(); j++) {
				selectExp = (SelectExpressionItem)selectItems.get(j);
				exp = selectExp.getExpression();
				Evaluator eval = new Evaluator(schema, tuple);
				exp.accept(eval);
				tupleList.add(new Datum(eval.getValue()));
			}
			newTuple = tupleList.toArray(new Datum[0]);
			return newTuple;
		}
	}

	@Override
	public void reset() {
		input.reset();
	}

	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return finalSchema;
	}
}
