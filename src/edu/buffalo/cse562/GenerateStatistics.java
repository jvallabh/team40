package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */

public class GenerateStatistics {
	int curNum = 0;
	Operator[] tableOperators;
	int[] filteredSize;
	int numOperators;
	ArrayList<Expression> whereCondExpressions;
	ArrayList<Expression> conditionsOnSingleTables;
	ArrayList<Datum[]> tupleList[];
	
	public GenerateStatistics(PlainSelect pselect, ArrayList[] partitionedConditions) {
		whereCondExpressions = partitionedConditions[0];
		conditionsOnSingleTables = partitionedConditions[1];
		
		List<SelectItem> selectItems= pselect.getSelectItems();
		FromScanner fromscan = new FromScanner(Main.dataDir, Main.tables);
		pselect.getFromItem().accept(fromscan);
		Operator firstTableOperator = fromscan.source;
		
		if(firstTableOperator instanceof IndexScanOperator){
			((IndexScanOperator)firstTableOperator).conditions = Util.getConditionsOfTable(firstTableOperator.getSchema(), conditionsOnSingleTables);
		}

		List<Join> joinDetails = pselect.getJoins();
		numOperators = joinDetails == null?0:joinDetails.size();
		tableOperators = new Operator[numOperators+1];
		filteredSize = new int[numOperators+1];
		tupleList = new ArrayList[numOperators+1];
		for (int i=0; i<numOperators+1; i++) {
			tupleList[i] = new ArrayList<Datum[]>();
		}
		
		tableOperators[curNum] = firstTableOperator;
		//filteredSize[curNum] = getFilteredSize(tableOperators[curNum]);
		curNum++;
		if (numOperators > 0) {
			for(Join currJoin:joinDetails){
				FromScanner tempFromScan = new FromScanner(Main.dataDir, Main.tables);
				currJoin.getRightItem().accept(tempFromScan);
				tableOperators[curNum] = tempFromScan.source;
				((IndexScanOperator)tableOperators[curNum]).conditions = Util.getConditionsOfTable(tableOperators[curNum].getSchema(), conditionsOnSingleTables);
				//filteredSize[curNum] = getFilteredSize(tableOperators[curNum]);
				curNum++;
			}
		}
		getFilteredSize();
		sortOperatorsOnSize();
		reorderOperators();
	}
	
	public void reorderOperators() {
		int tempSize;
		Operator tempOperator;
		ArrayList<Datum[]> tempList;
		for (int i=1; i<numOperators-1; i++) {
			int j = i+1;
			while ((!hasCommonCondition(i)) && j!=numOperators) {
				tempSize = filteredSize[j];
				filteredSize[j] = filteredSize[i];
				filteredSize[i] = tempSize;
				tempOperator = tableOperators[j];
				tableOperators[j] = tableOperators[i];
				tableOperators[i] = tempOperator;
				tempList = tupleList[j];
				tupleList[j] = tupleList[i];
				tupleList[i] = tempList;
				j++;
			}
		}

		System.out.println("After reordering");
		for (int i=0; i<numOperators+1; i++) {
			System.out.println(filteredSize[i]);
		}
	}
	
	public boolean hasCommonCondition(int index) {
		for (int i=0; i<index; i++) {
			Iterator<Expression> iterator = whereCondExpressions.iterator();
			ColumnInfo[] schema1, schema2;
			schema1 = tableOperators[index].getSchema();
			schema2 = tableOperators[i].getSchema();
			
			while(iterator.hasNext()){
				Expression currExp = iterator.next();
				if(!(currExp instanceof BinaryExpression)){
					continue;
				}
				Expression leftExp = ((BinaryExpression)currExp).getLeftExpression();
				Expression rightExp = ((BinaryExpression)currExp).getRightExpression();
				if(!(leftExp instanceof Column && rightExp instanceof Column)){
					continue;
				}
				Column leftColumn = (Column) leftExp;
				Column rightColumn = (Column) rightExp;
				if ((leftColumn.getTable().getName().equals(schema1[0].tableName) &&
				    rightColumn.getTable().getName().equals(schema2[0].tableName)) || 
				    (leftColumn.getTable().getName().equals(schema2[0].tableName) &&
					rightColumn.getTable().getName().equals(schema1[0].tableName))) {
					return true;
				}
			}
		}
		return false;
	}
	
	public void sortOperatorsOnSize() {
		int tempSize;
		Operator tempOperator;
		ArrayList<Datum[]> tempList;
		System.out.println("Before sort");
		for (int i=0; i<numOperators+1; i++) {
			System.out.println(filteredSize[i]);
		}
		
		for (int i=0; i<numOperators+1; i++) {
			for (int j=0; j<numOperators-i; j++) {
				if (filteredSize[j] > filteredSize[j+1]) {
					tempSize = filteredSize[j+1];
					filteredSize[j+1] = filteredSize[j];
					filteredSize[j] = tempSize;
					tempOperator = tableOperators[j+1];
					tableOperators[j+1] = tableOperators[j];
					tableOperators[j] = tempOperator;
					tempList = tupleList[i+1];
					tupleList[i+1] = tupleList[i];
					tupleList[i] = tempList;
				}
			}
		}
		System.out.println("After sort");
		for (int i=0; i<numOperators+1; i++) {
			System.out.println(filteredSize[i]);
		}
	}
	
	public void getFilteredSize() {
		for (int i=0; i<numOperators+1; i++) {
			int count = 0;
			Datum[] tuple = null;
			tuple = tableOperators[i].readOneTuple();
			while (tuple != null) {
				tupleList[i].add(tuple);
				count++;
				tuple = tableOperators[i].readOneTuple();
			} 
			filteredSize[i] = count;
		}
	}
}
