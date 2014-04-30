/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class OrderByOperator implements Operator {
	Operator input;
	List<OrderByElement> orderByColumns;
	int[] orderByColumnIndex;
	int[] orderIndex; //Here we store ascending or descending order. 1 for ASC, -1 for DESC
	ColumnInfo[] schema;
	ArrayList<Datum[]> orderedTuples = new ArrayList<>();
	ArrayList<SortableTuple> sortableTuples = new ArrayList<>();
	int readTupleIndex=0;
	
	OrderByOperator(Operator input, List<OrderByElement> orderByColumns){
		this.input=input;
		this.orderByColumns=orderByColumns;
		schema=input.getSchema();
		initOrderBy();
	}
	
	void initOrderBy(){
		getTuplesAsSortableList();
		getOrderByColumnIndexes();
		executeOrderBy();
	}
	
	private void executeOrderBy(){
		SortableTuple.schema=schema;
		SortableTuple.orderByColumnIndex=orderByColumnIndex;
		SortableTuple.orderIndex=orderIndex;
		Collections.sort(sortableTuples, new SortableTuple(null));
		for(SortableTuple curr:sortableTuples){
			orderedTuples.add(curr.tuple);
		}
		sortableTuples = null;
	}

	@Override
	public Datum[] readOneTuple() {
		if(readTupleIndex < orderedTuples.size()){
			return orderedTuples.get(readTupleIndex++);
		}
		return null;
	}

	@Override
	public void reset() {
		readTupleIndex = 0;
		
	}

	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}
	
	/**
	 * This method continuously reads tuples from input operator and stores it in a list.
	 */
	private void getTuplesAsSortableList(){
		Datum[] currTuple = null;
		while((currTuple = readOneTupleFromInput()) != null){
			SortableTuple sortableTuple = new SortableTuple(currTuple);
			sortableTuples.add(sortableTuple);
		}		
	}
	
	private Datum[] readOneTupleFromInput() {
		return input.readOneTuple();
	}
	
	/**
	 * This is a method to figure out the index of order by column in the schema.
	 * We also get the order of sorting (ASC/DESC)
	 */
	private void getOrderByColumnIndexes(){
		orderByColumnIndex = new int[orderByColumns.size()];
		orderIndex = new int[orderByColumns.size()];
		for(int i=0;i<orderByColumns.size();i++){
			if(orderByColumns.get(i).isAsc()){
				orderIndex[i]=1;
			}
			else{
				orderIndex[i]=-1;
			}
			Column currColumn = (Column)orderByColumns.get(i).getExpression();
			String currColumnName = currColumn.getColumnName();
			String currColumnTable = currColumn.getTable().getName();
			for(int j=0;j<schema.length;j++){
				boolean columnMatch = false;
				if(currColumnTable != null){
					columnMatch = schema[j].tableName.equalsIgnoreCase(currColumnTable) && schema[j].colDef.getColumnName().equalsIgnoreCase(currColumnName);
				}
				else if(schema[j].functionType==6){
					columnMatch = true;
				}
				else{
					columnMatch = schema[j].colDef.getColumnName().equalsIgnoreCase(currColumnName);
				}
				if(columnMatch){
					orderByColumnIndex[i]=j;
					break;
				}
			}
		}		
	}

}
