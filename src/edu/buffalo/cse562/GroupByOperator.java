/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Column;

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
	ColumnInfo[] schema;
	int grpByColumnIndex=-1;
	int currHashKeyIndex = 0;
	int currHashValueIndex = 0;
	
	int currReturnedGrpIndex = -1;
	
	GroupByOperator(Operator input, Column grpByColumn, ColumnInfo[] schema){
		this.input=input;
		this.grpByColumn=grpByColumn;
		this.schema=schema;
		doGroupBy();
	}
	
	private void doGroupBy(){
		//This is to get the index of our column
		if(grpByColumn.getTable().getName() != null){
			String grpByColumnTable = grpByColumn.getTable().getName();
			String columnName = grpByColumn.getColumnName();
			for(int i=0;i<schema.length;i++){
				boolean isSameColumnName = schema[i].colDef.getColumnName().equalsIgnoreCase(columnName) || schema[i].colDef.getColumnName().equalsIgnoreCase(grpByColumnTable+"."+columnName);
				if(schema[i].tableName.equalsIgnoreCase(grpByColumnTable) && isSameColumnName){
					grpByColumnIndex = i;
					break;
				}
			}
			//columnDef = Util.getColumnDefinitionOfColumn(grpByColumn);
		}
		else{
			for(int i=0;i<schema.length;i++){
				if(schema[i].colDef.getColumnName().equalsIgnoreCase(grpByColumn.getColumnName())){
					grpByColumnIndex = i;
					break;
				}
			}			
		}
		//Following code is to handle the scenarios, where we need to group by multiple columns
		if(input instanceof GroupByOperator){
			Entry<String, ArrayList<Datum[]>> currGrp = null;
			while((currGrp = ((GroupByOperator) input).readOneGroup()) != null){
				String grpKey = currGrp.getKey();
				ArrayList<Datum[]> grpList = currGrp.getValue();
				LinkedHashMap<String, ArrayList<Datum[]>> tempGroupedMap = new LinkedHashMap<>();
				for(int i=0;i<grpList.size();i++){
					Datum[] currTuple = grpList.get(i);
					String tempGrpKey = currTuple[grpByColumnIndex].String();
					if(tempGroupedMap.containsKey(tempGrpKey)){
						tempGroupedMap.get(tempGrpKey).add(currTuple);
					}
					else{
						ArrayList<Datum[]> datumList = new ArrayList<>();
						datumList.add(currTuple);
						tempGroupedMap.put(tempGrpKey, datumList);
					}
				}
				ArrayList<Datum[]> finalGroupedList = new ArrayList<>();
				Iterator<Entry<String, ArrayList<Datum[]>>> iterator = tempGroupedMap.entrySet().iterator();
				while(iterator.hasNext()){
					finalGroupedList.addAll(iterator.next().getValue());
				}
				groupedMap.put(grpKey, finalGroupedList);				
			}
		}
		else{			
			//When we are doing the group by for the first time
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
		}		
	}

	@Override
	public Datum[] readOneTuple() {
		//System.out.println("Grouped map size is: "+groupedMap.size());
		if(currHashKeyIndex>=groupedMap.size()){
			return null;
		}
		Object[] hashMapArray = groupedMap.values().toArray();
		@SuppressWarnings("unchecked")
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
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
	
	@SuppressWarnings("unchecked")
	public Entry<String, ArrayList<Datum[]>> readOneGroup(){
		currReturnedGrpIndex++;
		if(currReturnedGrpIndex<groupedMap.size()){
			Object[] hashMapArray = groupedMap.entrySet().toArray();
			//Object[] hashMapArray = groupedMap.values().toArray();
			return (Entry<String, ArrayList<Datum[]>>) hashMapArray[currReturnedGrpIndex];
		}
		return null;
	}
	
	public void resetReturnGrp() {
		currReturnedGrpIndex = -1;
	}

}
