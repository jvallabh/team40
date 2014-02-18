/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class GroupByOperator implements Operator {
	Operator input = null;
	Column grpByColumn;
	LinkedHashMap<String, ArrayList<Datum[]>> groupedMap = new LinkedHashMap<>();
	ColumnDefinition[] schema;
	int grpByColumnIndex=-1;
	int currHashKeyIndex = 0;
	int currHashValueIndex = 0;
	
	GroupByOperator(Operator input, Column grpByColumn, ColumnDefinition[] schema){
		this.input=input;
		this.grpByColumn=grpByColumn;
		this.schema=schema;
		doGroupBy();
	}
	
	private void doGroupBy(){
		//This is to get the index of our column
		ColumnDefinition columnDef = null;
		if(grpByColumn.getTable().getName() != null){
			columnDef = Main.getColumnDefinitionOfColumn(grpByColumn);
		}
		for(int i=0;i<schema.length;i++){
			//System.out.println("schema[i] is: "+schema[i]+" grpByColumn is: "+grpByColumn);
			boolean testCondition = columnDef != null?schema[i]==columnDef:schema[i].getColumnName().equals(grpByColumn.getColumnName());
			if(testCondition){
				grpByColumnIndex = i;
				break;
			}
		}
		Datum[] currTuple = null;
		while((currTuple = input.readOneTuple()) != null){
			if(groupedMap.containsKey(currTuple[grpByColumnIndex].String())){
				groupedMap.get(currTuple[grpByColumnIndex].String()).add(currTuple);
			}
			else{
				ArrayList<Datum[]> datumList = new ArrayList<>();
				datumList.add(currTuple);
				groupedMap.put(currTuple[grpByColumnIndex].String(), datumList);
			}
		}
		//System.out.println("Printing hashMap");
		//System.out.println(groupedMap);
		//System.out.println(groupedMap.size());
		
	}

	@Override
	public Datum[] readOneTuple() {
		if(currHashKeyIndex>=groupedMap.size()){
			return null;
		}
		Object[] hashMapArray = groupedMap.values().toArray();
		ArrayList<Datum[]> currGroupedList = (ArrayList<Datum[]>) hashMapArray[currHashKeyIndex];
		Datum[] currTuple=currGroupedList.get(currHashValueIndex);
		currHashValueIndex++;
		if(currHashValueIndex==currGroupedList.size()){
			currHashKeyIndex++;
			currHashValueIndex = 0;
		}
		
		// TODO Auto-generated method stub
		return currTuple;
	}

	@Override
	public void reset() {
		currHashKeyIndex = 0;
		currHashValueIndex = 0;		
	}

	@Override
	public ColumnDefinition[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}
