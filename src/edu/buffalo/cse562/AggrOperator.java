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
	ColumnInfo[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	Datum[] result=null;
	int[] selectItemType; 
	int resultCount =0;
	private static int sum=1,avg=2,count=3,min=4,max=5;
	public AggrOperator(Operator input, ColumnInfo[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		this.selectItemType= getSelectItemType(selectItems);
	}
	
	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].colDef.getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public Datum[] readOneTuple() {
		if(result==null){
		result = input.readOneTuple();
		resultCount++;
		}
		while(result!=null){
		Datum[] tuple = input.readOneTuple();
		if(tuple==null){
			Datum[] tmp = result;
			result =null;
			return getResult(tmp);
		}
		resultCount++;
		for(int i=0; i<tuple.length;i++)
		{
			if(selectItemType[i]==0 && !result[i].element.equals(tuple[i].element)) {
				Datum [] tmp = result;
				result =tuple;
				return getResult(tmp);
			}
			else{
				getUpdate(tuple);

			}
		  }		
		}
		return getResult(result);
	}
	
	public Datum[] getResult(Datum[] tuple)
	{
		if(tuple==null)
			return null;
		for(int i=0; i<tuple.length;i++)
		{
			if(selectItemType[i]==avg)
			result[i].element = Integer.toString(Integer.parseInt(result[i].toString()) /resultCount);			
		}
		resultCount = 0;
		return result;
	}
	
	public void getUpdate(Datum[] tuple){
		if(tuple==null)
			return;
		for(int i=0; i<tuple.length;i++)
		{
			if(selectItemType[i]==sum)
			result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema);
			else if(selectItemType[i]==min)
				result[i].element = Math.getMin(result[i].toString(), tuple[i].toString(),i,schema);
			else if(selectItemType[i]==max)
				result[i].element = Math.getMax(result[i].toString(), tuple[i].toString(),i,schema);
			else if(selectItemType[i]==avg)
				result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema);
			else if(selectItemType[i]==count)
				result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema);
						
		}
	}
	

	@Override
	public void reset() {
		input.reset();
	}
	
	public static int[] getSelectItemType(List<SelectItem> selectItems ){
		int[] selectItemType = new int[selectItems.size()];
		if (selectItems.get(0) instanceof AllColumns)
			return selectItemType;
		for (int j=0; j<selectItems.size(); j++) {
			SelectExpressionItem selectExp = (SelectExpressionItem)selectItems.get(j);
			Expression exp = selectExp.getExpression();
			if ( exp instanceof Function){
			Function f= (Function)exp;	
			if(f.getName().toString().equalsIgnoreCase("SUM"))
				selectItemType[j] =sum;
			if(f.getName().toString().equalsIgnoreCase("MIN"))
				selectItemType[j] =min;
			if(f.getName().toString().equalsIgnoreCase("MAX"))
				selectItemType[j] =max;
			if(f.getName().toString().equalsIgnoreCase("AVG"))
				selectItemType[j] =avg;
			if(f.getName().toString().equalsIgnoreCase("COUNT"))
				selectItemType[j] =count;
			}
			else
				selectItemType[j] =0;
			
		}
		return selectItemType;
	}
	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}