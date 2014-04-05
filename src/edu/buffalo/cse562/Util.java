/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
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
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

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
	
	/**
	 * Reads tuples from the inputOperator and prints it to the console
	 * @param inputOperator - operator to read tuples from
	 * @param count - limit on the number of tuples to print
	 */
	public static void printOutputTuples(Operator inputOperator, int count){
		Datum[] currTuple = null;
		int output = 0;
		while(output < count && (currTuple = inputOperator.readOneTuple()) != null){
			String currDatumString = "";
			for(Datum currDatum:currTuple){
				currDatumString = currDatumString+"|"+currDatum;
			}
			System.out.println(currDatumString.substring(1));
			output++;
		}
		
	}
	
	public static Operator getJoinedOperator(Operator firstTable, List<Join> joinDetails, ArrayList<Expression> conditionsOnSingleTables, ArrayList<Expression> whereCondExpressions){
		JoinOperator finalJoinedOperator = null;
		for(Join currJoin:joinDetails){
			FromScanner tempFromScan = new FromScanner(Main.dataDir, Main.tables);
			currJoin.getRightItem().accept(tempFromScan);
			Operator tempTableOperator = tempFromScan.source;
			((ScanOperator)tempTableOperator).conditions = Util.getConditionsOfTable(tempTableOperator.getSchema(), conditionsOnSingleTables);
			if(currJoin.isSimple()){
				if(finalJoinedOperator == null){
					Object[] whereJoinConditionDetails = getConditionsOfJoin(firstTable.getSchema(), tempTableOperator.getSchema(), whereCondExpressions);
					int index1 =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[0];
					ExternalSortOperator sortOp1 = new ExternalSortOperator(firstTable, null, Main.swapDir,index1);
					
					int index2 =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[1];
					ExternalSortOperator sortOp2 = new ExternalSortOperator(tempTableOperator, null, Main.swapDir,index2);
					
					
					finalJoinedOperator = new JoinOperator(sortOp1, sortOp2, null);					
					finalJoinedOperator.whereJoinCondition = (ArrayList<Expression>)whereJoinConditionDetails[0];
					finalJoinedOperator.whereJoinIndexes = (ArrayList<Integer[]>)whereJoinConditionDetails[1];
					finalJoinedOperator.buildHash();
				}
				else{
					ColumnInfo[] schema1 = finalJoinedOperator.getSchema();
					Object[] whereJoinConditionDetails = getConditionsOfJoin(schema1, tempTableOperator.getSchema(), whereCondExpressions);
					int index =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[1];
					ExternalSortOperator sortOp2 = new ExternalSortOperator(tempTableOperator, null, Main.swapDir, index);					
					
					
					finalJoinedOperator = new JoinOperator(finalJoinedOperator, sortOp2, null);

					finalJoinedOperator.whereJoinCondition = (ArrayList<Expression>)whereJoinConditionDetails[0];
					finalJoinedOperator.whereJoinIndexes = (ArrayList<Integer[]>)whereJoinConditionDetails[1];
					finalJoinedOperator.buildHash();
				}
			}
			Expression joinCondition = currJoin.getOnExpression();
			if(joinCondition != null){
				if(finalJoinedOperator == null){
					Object[] whereJoinConditionDetails = getConditionsOfJoin(firstTable.getSchema(), tempTableOperator.getSchema(), whereCondExpressions);
					int index =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[0];
					ExternalSortOperator sortOp1 = new ExternalSortOperator(firstTable, null, Main.swapDir,index);
					
					index =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[1];
					ExternalSortOperator sortOp2 = new ExternalSortOperator(tempTableOperator, null, Main.swapDir,index);
					
					
					finalJoinedOperator = new JoinOperator(sortOp1, sortOp2, joinCondition);
					finalJoinedOperator.whereJoinCondition = (ArrayList<Expression>)whereJoinConditionDetails[0];
					finalJoinedOperator.whereJoinIndexes = (ArrayList<Integer[]>)whereJoinConditionDetails[1];
					finalJoinedOperator.buildHash();
				}
				else{
					
					Object[] whereJoinConditionDetails = getConditionsOfJoin(finalJoinedOperator.getSchema(), tempTableOperator.getSchema(), whereCondExpressions);
					int index =( (ArrayList<Integer[]>) whereJoinConditionDetails[1]).get(0)[1];
					ExternalSortOperator sortOp2 = new ExternalSortOperator(tempTableOperator, null, Main.swapDir,index);					
					
					
					finalJoinedOperator = new JoinOperator(finalJoinedOperator, sortOp2, joinCondition);
					finalJoinedOperator.whereJoinCondition = (ArrayList<Expression>)whereJoinConditionDetails[0];
					finalJoinedOperator.whereJoinIndexes = (ArrayList<Integer[]>)whereJoinConditionDetails[1];
					finalJoinedOperator.buildHash();
				}				
			}
		}
		return finalJoinedOperator;
	}
	
	public static Operator getGroupByOperator(Operator inputOperator, List<Column> groupByColumns){
		//OrderByOperator needs columns to be in the form of OrderByElement object. So, we are using a helper method for conversion. 
		List<OrderByElement> orderByElements = convertGrpByColumnsToOrderByElements(groupByColumns);
		return new OrderByOperator(inputOperator, orderByElements);
		//Commented out below piece of code as we decided to use orderBy operator logic for group by as well.
		/*GroupByOperator finalGrpByOperator = null;
		for(Column currColumn:groupByColumns){
			if(finalGrpByOperator == null){
				finalGrpByOperator = new GroupByOperator(inputOperator, currColumn, inputOperator.getSchema());
			}
			else{
				finalGrpByOperator = new GroupByOperator(finalGrpByOperator, currColumn, finalGrpByOperator.getSchema());
			}			
		}
		return finalGrpByOperator;*/
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
	public static ArrayList[] partitionWhereClause(Expression where){
		ArrayList<Expression> conditionsOnSingleTables = new ArrayList<>();
		ArrayList<Expression> whereCondExpressions = new ArrayList<>();
		Expression currExpression = where;
		while(currExpression instanceof Expression){
			if(currExpression instanceof AndExpression){
				AndExpression andExp = (AndExpression) currExpression;
				Expression rightExp = andExp.getRightExpression();
				if(isSingleTableConditionExpression(rightExp)){
					conditionsOnSingleTables.add(rightExp);
				}
				else{
					whereCondExpressions.add(rightExp);
				}
				currExpression = andExp.getLeftExpression();				
			}
			else if(isSingleTableConditionExpression(currExpression)){
				conditionsOnSingleTables.add(currExpression);
				break;
			}
			else{
				whereCondExpressions.add(currExpression);
				break;
			}
		}
		//System.out.println("whereCondExpressions are: "+whereCondExpressions+" conditionsOnSingleTables are: "+conditionsOnSingleTables);
		ArrayList[] partitionedConditions = {whereCondExpressions, conditionsOnSingleTables};
		return partitionedConditions;
		
		//This is to handle the scenario SELECT * FROM R,S WHERE R.A='123'; 
		//Since here WHERE clause is not an AndExpression, above while loop would have bypassed. So, handling that case here.
		
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
			/*This is to handle conditions like
			  ps1.supplycost = ( 
			  SELECT min(ps2.supplycost) 
	          FROM partsupp ps2, supplier s2, nation n2, region r2
	          WHERE
	              p1.partkey = ps2.partkey
	              AND s2.suppkey = ps2.suppkey 
	              AND s2.nationkey = n2.nationkey 
	              AND n2.regionkey = r2.regionkey 
	              AND r2.name = 'EUROPE'
	          )*/
			if(inputExp instanceof EqualsTo && currRight instanceof SubSelect){
				SelectBody select = ((SubSelect) currRight).getSelectBody();
				if(select instanceof PlainSelect){
					PlainSelect pselect = (PlainSelect)select;
					Datum[] resultTuple = SelectProcessor.processPlainSelect(pselect).readOneTuple();
					((EqualsTo)inputExp).setRightExpression(new LongValue(resultTuple[0].String()));
				}
			}
			return true;
		}
		else if(currLeft instanceof Column && currRight instanceof Column){
			String leftTableName = ((Column)currLeft).getTable().getName();
			String rightTableName = ((Column)currRight).getTable().getName();
			if(leftTableName == null && rightTableName == null){
				return true;
			}
			else if(leftTableName.equalsIgnoreCase(rightTableName)){
				return true;
			}
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
	public static ArrayList<Expression> getConditionsOfTable(ColumnInfo[] schema, ArrayList<Expression> conditionsOnSingleTables){
		ArrayList<Expression> output = new ArrayList<>();
		Iterator<Expression> iterator = conditionsOnSingleTables.iterator();
		while(iterator.hasNext()){
			Expression currExp = iterator.next();
			Column currColumn = (Column) ((BinaryExpression)currExp).getLeftExpression();
			for (int i=0; i<schema.length; i++) {
				if (schema[i].colDef.getColumnName().equals(currColumn.getColumnName())) {
					if(currColumn.getTable().getName() != null){
						if(!currColumn.getTable().getName().equals(schema[i].tableName)){
							continue;
						}						
					}
					output.add(currExp);
					break;
				}
			}
		}
		return output;
	}
	
	/**
	 * This method can be used in converting cross product to a join.
	 * SELECT * FROM R,S WHERE R.B=S.B AND R.B=(S.B+0.2);
	 * schema1, schema2 are schemas of R,S.
	 * whereCondExpressions are [R.B=S.B, R.B=(S.B+0.2)]
	 * This method will return [R.B=S.B] as output.
	 * We are selecting expressions of form ColumnLeft = ColumnRight. First we look for ColumnLeft/ColumnRight in schema1, if we find one then we look for ColumnRight/ColumnLeft in schema2.  
	 * @param schema1
	 * @param schema2
	 * @param whereCondExpressions
	 * @return
	 */
	public static Object[] getConditionsOfJoin(ColumnInfo[] schema1, ColumnInfo[] schema2, ArrayList<Expression> whereCondExpressions){
		//joinExp variable contains the join expressions like R.C=S.C 
		ArrayList<Expression> joinExp = new ArrayList<>();
		//Here we are maintaining column indexes of join expressions. If the join condition is like R.C=S.B then we fetch the indexes of C from table R, B from table S 
		ArrayList<Integer[]> joinExpIndexes = new ArrayList<>();
		Iterator<Expression> iterator = whereCondExpressions.iterator();
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
			Integer[] currExpIndexes = new Integer[2];
			for (int i=0; i<schema1.length; i++) {
				if (schema1[i].colDef.getColumnName().equals(leftColumn.getColumnName())) {
					if(leftColumn.getTable().getName() != null){
						if(leftColumn.getTable().getName().equals(schema1[i].tableName)){
							for (int j=0; j<schema2.length; j++) {
								if (schema2[j].colDef.getColumnName().equals(rightColumn.getColumnName())) {
									if(rightColumn.getTable().getName() != null){
										if(!rightColumn.getTable().getName().equals(schema2[j].tableName)){
											continue;
										}						
									}							
									joinExp.add(currExp);
									currExpIndexes[0] = i;
									currExpIndexes[1] = j;
									joinExpIndexes.add(currExpIndexes);
									iterator.remove();
									break;
								}
							}
						}						
					}
				}
				if (schema1[i].colDef.getColumnName().equals(rightColumn.getColumnName())) {
					if(rightColumn.getTable().getName() != null){
						if(rightColumn.getTable().getName().equals(schema1[i].tableName)){
							for (int j=0; j<schema2.length; j++) {
								if (schema2[j].colDef.getColumnName().equals(leftColumn.getColumnName())) {
									if(leftColumn.getTable().getName() != null){
										if(!leftColumn.getTable().getName().equals(schema2[j].tableName)){
											continue;
										}						
									}							
									joinExp.add(currExp);
									currExpIndexes[0] = i;
									currExpIndexes[1] = j;
									joinExpIndexes.add(currExpIndexes);
									iterator.remove();
									break;
								}
							}
						}						
					}
				}
			}
		}
		return new Object[]{joinExp, joinExpIndexes};
	}
	
	/**
	 * This method converts a list of columns into a list of OrderByElements
	 * @param grpByCols
	 * @return
	 */
	public static List<OrderByElement> convertGrpByColumnsToOrderByElements(List<Column> grpByCols){
		List<OrderByElement> orderByElements = new ArrayList<>();
		Iterator<Column> iterator = grpByCols.iterator();
		OrderByElement orderByElement = null;
		while(iterator.hasNext()){
			orderByElement = new OrderByElement();
			orderByElement.setExpression(iterator.next());
			orderByElement.setAsc(true);
			orderByElements.add(orderByElement);
		}
		return orderByElements;
	}
}
