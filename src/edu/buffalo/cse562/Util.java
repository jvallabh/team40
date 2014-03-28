/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
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
	public static ArrayList<Expression> conditionsOnSingleTables = new ArrayList<>();
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
	
	/**
	 * It will process WHERE clause and stores the conditions involving only single table separately.
	 * Example : SELECT * FROM R,S WHERE R.A='123' AND S.B='456' AND R.C = S.C;
	 * It fetches R.A='123', S.B='456' and stores them in a list.
	 * @param where
	 */
	public static void partitionWhereClause(Expression where){
		Expression currExpression = where;		
		while(currExpression instanceof AndExpression){
			AndExpression andExp = (AndExpression) currExpression;
			if(isSingleTableConditionExpression(andExp.getRightExpression())){
				conditionsOnSingleTables.add(andExp.getRightExpression());
			}
			currExpression = andExp.getLeftExpression();
		}
		
		//This is to handle the scenario SELECT * FROM R,S WHERE R.A='123'; 
		//Since here WHERE clause is not an AndExpression, above while loop would have bypassed. So, handling that case here.
		if(isSingleTableConditionExpression(currExpression)){
			conditionsOnSingleTables.add(currExpression);
			return;
		}
	}
	
	/**
	 * This method will for presence of only one table in the given expression.
	 * Example: For the input, R.B='123', it will return true.
	 * For the input, R.C=S.C, it will return false.
	 * @param inputExp
	 * @return
	 */
	public static boolean isSingleTableConditionExpression(Expression inputExp){
		Expression currLeft = null, currRight = null;
		if(inputExp instanceof EqualsTo){
			currLeft = ((EqualsTo) inputExp).getLeftExpression();
			currRight = ((EqualsTo) inputExp).getRightExpression();
		}
		else if(inputExp instanceof NotEqualsTo){
			currLeft = ((NotEqualsTo) inputExp).getLeftExpression();
			currRight = ((NotEqualsTo) inputExp).getRightExpression();			
		}
		else if(inputExp instanceof GreaterThan){
			currLeft = ((GreaterThan) inputExp).getLeftExpression();
			currRight = ((GreaterThan) inputExp).getRightExpression();			
		}
		else if(inputExp instanceof GreaterThanEquals){
			currLeft = ((GreaterThanEquals) inputExp).getLeftExpression();
			currRight = ((GreaterThanEquals) inputExp).getRightExpression();			
		}
		else if(inputExp instanceof MinorThan){
			currLeft = ((MinorThan) inputExp).getLeftExpression();
			currRight = ((MinorThan) inputExp).getRightExpression();			
		}
		else if(inputExp instanceof MinorThanEquals){
			currLeft = ((MinorThanEquals) inputExp).getLeftExpression();
			currRight = ((MinorThanEquals) inputExp).getRightExpression();			
		}
		
		if(currLeft instanceof Column && !(currRight instanceof Column)){
			return true;
		}
		return false;		
	}
	
	/**
	 * Given a schema, it will return the list of conditions that can be evaluated.
	 * Example : SELECT * FROM R,S WHERE R.A='123' AND S.B='456' AND R.C = S.C;
	 * When you send the schema of table R, this method will return the condition R.A='123' only.  
	 * @param schema
	 * @return
	 */
	public static ArrayList<Expression> getConditionsOfTable(ColumnInfo[] schema){
		ArrayList<Expression> output = new ArrayList<>();
		Iterator<Expression> iterator = conditionsOnSingleTables.iterator();
		while(iterator.hasNext()){
			Expression currExp = iterator.next();
			Column currColumn = (Column) ((BinaryExpression)currExp).getLeftExpression();
			for (int i=0; i<schema.length; i++) {
				if (schema[i].colDef.getColumnName().equals(currColumn.getColumnName())) {
					output.add(currExp);
					break;
				}
			}
		}
		return output;
	}
}
