/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
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
	
	public ProjectionOperator(Operator input, ColumnDefinition[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
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
	public Datum[] readOneTuple() {
		Datum[] tuple = input.readOneTuple();
		ArrayList<Datum> tupleList = new ArrayList<>();
		Datum[] newTuple = null;
		if(tuple == null)
			return null;
		if (selectItems.get(0) instanceof AllColumns)
			return tuple;
		else {			
			Column col;
			SelectExpressionItem selectExp;
			Expression exp = null;
			for (int j=0; j<selectItems.size(); j++) {
				selectExp = (SelectExpressionItem)selectItems.get(j);
				exp = selectExp.getExpression();
				if (exp instanceof Column) {
					col = (Column)exp;
					tupleList.add(tuple[getColumnID(col)]);
				} else if (exp instanceof BinaryExpression) {
					Evaluator eval = new Evaluator(schema, tuple);
					if(exp != null){
						exp.accept(eval);
						// TODO Have to get the values based on the data-type
						tupleList.add(new Datum(Integer.toString(eval.getIntValue())));
					}
				}
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
		return null;
	}

}
