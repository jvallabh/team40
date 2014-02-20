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
	ColumnDefinition[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	ColumnDefinition[] finalSchema;
	
	public ProjectionOperator(Operator input, ColumnDefinition[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		this.finalSchema = changeSchema(selectItems);
	}
	
	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	
	public ColDataType getNumType(Expression expr) {
		if (expr instanceof BinaryExpression) {
			ColDataType colDataType = getNumType(((BinaryExpression) expr).getLeftExpression());
			if (colDataType == null) {
				return getNumType(((BinaryExpression) expr).getRightExpression());
			}
			return colDataType;	
		}
		else if (expr instanceof Column) {
			Column col = (Column) expr;
			return (schema[getColumnID(col)].getColDataType());
		}
		return null;
	}
	
	public ColumnDefinition[] changeSchema(List selectItems) {
		ColumnDefinition[] schema = new ColumnDefinition[selectItems.size()];
		SelectExpressionItem exp;
		for (int j=0; j<selectItems.size(); j++) {
			exp = (SelectExpressionItem)selectItems.get(j);
			if(exp.getAlias()!=null) {
				schema[j] = new ColumnDefinition();
				schema[j].setColumnName(exp.getAlias());
			}
			else {
				schema[j] = new ColumnDefinition();
				schema[j].setColumnName(exp.toString());
			}
			schema[j].setColDataType(getNumType(exp.getExpression()));
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
	public ColumnDefinition[] getSchema() {
		// TODO Auto-generated method stub
		return finalSchema;
	}
}
