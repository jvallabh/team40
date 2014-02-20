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
import net.sf.jsqlparser.expression.Function;
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
public class AggrOperator implements Operator {
	Operator input;
	ColumnDefinition[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	Datum[] result=null;
	
	public AggrOperator(Operator input, ColumnDefinition[] schema, List<SelectItem> selectItems) {
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
		if(result==null)
		result = input.readOneTuple(); 
		while(result!=null){
		Datum[] tuple = input.readOneTuple();
		if(tuple==null){
			Datum[] tmp = result;
			result =null;
			return tmp;
		}
		int[] selectItemType = getSelectItemType(selectItems);
		for(int i=0; i<tuple.length;i++)
		{
			if(selectItemType[i]==0 && !result[i].element.equals(tuple[i].element)) {
				Datum [] tmp = result;
				result =tuple;
				return tmp;
			}
			else if(selectItemType[i]==1){
result[i].element = Integer.toString((Integer.parseInt(result[i].toString()) + Integer.parseInt(tuple[i].toString())));
			}
		  }		
		}
		return result;
	}

	@Override
	public void reset() {
		input.reset();
	}
	public static int[] getSelectItemType(List<SelectItem> selectItems ){
		int[] selectItemType = new int[selectItems.size()];
		for (int j=0; j<selectItems.size(); j++) {
			SelectExpressionItem selectExp = (SelectExpressionItem)selectItems.get(j);
			Expression exp = selectExp.getExpression();
			if ( exp instanceof Function)
				selectItemType[j] =1;
			else
				selectItemType[j] =0;
			
		}
		return selectItemType;
	}
	@Override
	public ColumnDefinition[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}