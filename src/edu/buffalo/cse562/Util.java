/**
 * 
 */
package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class Util {
	/**
	 * Reads tuples from the inputOperator and prints it to the console
	 * @param inputOperator
	 */
	public static void printOutputTuples(Operator inputOperator){
		Datum[] currTuple = null;
		while((currTuple = inputOperator.readOneTuple()) != null){
			String currDatumString = "";
			for(Datum currDatum:currTuple){
				currDatumString = currDatumString+"|"+currDatum;
			}
			System.out.println(currDatumString.substring(1));
		}
		
	}
	
	public static Operator getJoinedOperator(Operator firstTable, List<Join> joinDetails){
		JoinOperator finalJoinedOperator = null;
		for(Join currJoin:joinDetails){
			FromScanner tempFromScan = new FromScanner(Main.dataDir, Main.tables);
			currJoin.getRightItem().accept(tempFromScan);
			Operator tempTableOperator = tempFromScan.source;
			if(currJoin.isSimple()){
				if(finalJoinedOperator == null){
					finalJoinedOperator = new JoinOperator(firstTable, tempTableOperator, null);
				}
				else{
					finalJoinedOperator = new JoinOperator(finalJoinedOperator, tempTableOperator, null);
				}
			}
			Expression joinCondition = currJoin.getOnExpression();
			if(joinCondition != null){
				if(finalJoinedOperator == null){
					finalJoinedOperator = new JoinOperator(firstTable, tempTableOperator, joinCondition);
				}
				else{
					finalJoinedOperator = new JoinOperator(finalJoinedOperator, tempTableOperator, joinCondition);
				}				
			}
		}
		return finalJoinedOperator;
	}
	
	public static Operator getGroupByOperator(Operator inputOperator, List<Column> groupByColumns){
		GroupByOperator finalGrpByOperator = null;
		for(Column currColumn:groupByColumns){
			if(finalGrpByOperator == null){
				finalGrpByOperator = new GroupByOperator(inputOperator, currColumn, inputOperator.getSchema());
			}
			else{
				finalGrpByOperator = new GroupByOperator(finalGrpByOperator, currColumn, finalGrpByOperator.getSchema());
			}			
		}
		return finalGrpByOperator;
	}
	
	@SuppressWarnings("unchecked")
	public static ColumnDefinition getColumnDefinitionOfColumn(Column column){
		//System.out.println("Curr table name is: "+column.getTable().getName()+" "+column.getTable().getAlias()+" "+column.getTable().getSchemaName()+" "+column.getTable().getWholeTableName());
		CreateTable currTable = Main.tables.get(column.getTable().getName());
		List<ColumnDefinition> colDefList = currTable.getColumnDefinitions();
		ColumnDefinition columnDef=null;
		for(ColumnDefinition currDef:colDefList){
			if(column.getColumnName().equals(currDef.getColumnName())){
				columnDef = currDef;
				break;
			}
		}
		return columnDef;		
	}

}
