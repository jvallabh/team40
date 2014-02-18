package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class Main {
	public static File dataDir;
	public static HashMap<String, CreateTable> tables;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i;
		ArrayList<File> sqlFiles = new ArrayList<>();
		tables = new HashMap<>();
		for(i=0;i<args.length;i++){
			if(args[i].equals("--data")){
				dataDir = new File(args[i+1]);
				i++;
			}
			else{
				sqlFiles.add(new File(args[i]));
			}
		}
		for(File sql:sqlFiles){
			FileReader stream = null;
			CCJSqlParser parser = null;
			try{
				stream = new FileReader(sql);
				parser = new CCJSqlParser(stream);
				Statement stmt;				
				while((stmt = parser.Statement()) != null){
					if(stmt instanceof CreateTable){
						CreateTable currTable = (CreateTable)stmt;
						String tableName = currTable.getTable().getName();
						tables.put(tableName, currTable);
					}
					else if(stmt instanceof Select){
						SelectBody select = ((Select)stmt).getSelectBody();
						if(select instanceof PlainSelect){
							PlainSelect pselect = (PlainSelect)select;
							Expression selectCondition = pselect.getWhere();
							List<SelectItem> selectItems= pselect.getSelectItems();
							FromScanner fromscan = new FromScanner(dataDir, tables);
							pselect.getFromItem().accept(fromscan);
							Operator firstTableOperator = fromscan.source;
														
							/*
							 * If JOIN is present in the SQL query then first we are fetching the list of JOIN tables.
							 * Then for each table we are joining them based on the join condition.
							 */
							List<Join> joinDetails = pselect.getJoins();
							boolean hasJoin = joinDetails == null?false:true;
							JoinOperator finalJoinedOperator = null;
							
							List<Column> groupByColumns = pselect.getGroupByColumnReferences();
							boolean hasGroupBy = groupByColumns == null?false:true;
							GroupByOperator finalGrpByOperator = null;
							
							SelectionOperator selectOperator = null;
							ProjectionOperator projectOperator = null;
							
							if(hasJoin){
								finalJoinedOperator = (JoinOperator) getJoinedOperator(firstTableOperator, joinDetails);
								selectOperator = new SelectionOperator(finalJoinedOperator, finalJoinedOperator.schema, selectCondition);
							}
							//This portion of code is to get the name of table immediately after FROM, that is first table name in the SQL query
							else{
								TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
								List tableList = tablesNamesFinder.getTableList((Select)stmt);
								String tableName = null;
								for (Iterator iter = tableList.iterator(); iter.hasNext();) {
								  tableName = String.valueOf(iter.next());
								  break;
								}
								CreateTable currTableObject = tables.get(tableName);
								ColumnDefinition[] schema = new ColumnDefinition[currTableObject.getColumnDefinitions().size()]; 
								currTableObject.getColumnDefinitions().toArray(schema);
								selectOperator = new SelectionOperator(firstTableOperator, schema, selectCondition);
							}
							
							if(hasGroupBy){
								finalGrpByOperator = (GroupByOperator) getGroupByOperator(selectOperator, groupByColumns);
							}
							
							Operator inputToProject = finalGrpByOperator!=null?finalGrpByOperator:selectOperator;
							projectOperator = new ProjectionOperator(inputToProject,inputToProject.getSchema(),selectItems);
							printOutputTuples(projectOperator);
							
						}
					}
				}
			}
			catch(ParseException e){
				e.printStackTrace();
			}
			catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
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
			FromScanner tempFromScan = new FromScanner(dataDir, tables);
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
	
	public static ColumnDefinition getColumnDefinitionOfColumn(Column column){
		//System.out.println("Curr table name is: "+column.getTable().getName()+" "+column.getTable().getAlias()+" "+column.getTable().getSchemaName()+" "+column.getTable().getWholeTableName());
		CreateTable currTable = tables.get(column.getTable().getName());
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
