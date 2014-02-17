/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
	ArrayList<Integer> itemList = new ArrayList();
	
	public ProjectionOperator(Operator input, ColumnDefinition[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		Column col;
		SelectExpressionItem selectExp;
		if(selectItems.get(0) instanceof SelectExpressionItem) {
			for (int j=0; j<selectItems.size(); j++) {
				selectExp = (SelectExpressionItem)selectItems.get(j);
				col = (Column)selectExp.getExpression();
				for(int i=0;i<schema.length;i++){
					if (schema[i].getColumnName().equals(col.getColumnName())) {
						this.itemList.add(i);
					}
				}
			}
		}
	}
	@Override
	public Datum[] readOneTuple() {
		ArrayList<Datum> tupleList = new ArrayList();
		Datum[] tupleTemp = null;
		Datum[] tuple = null;
		tupleTemp = input.readOneTuple();
		if(tupleTemp==null)
			return null;
		else {
			if(!itemList.isEmpty()){
				for (int i=0; i<itemList.size(); i++) {
					tupleList.add(tupleTemp[itemList.get(i)]);
				}
			tuple = tupleList.toArray(new Datum[0]);
			}
			else if (selectItems.get(0) instanceof AllColumns){
				tuple = tupleTemp;
			}
		}
		return tuple;
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
