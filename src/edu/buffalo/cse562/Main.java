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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i;
		File dataDir = null;
		ArrayList<File> sqlFiles = new ArrayList<>();
		HashMap<String, CreateTable> tables = new HashMap<>();
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
						//currTable.getColumnDefinitions()
						System.out.println("Column definitions are: "+currTable.getColumnDefinitions());
						ListIterator<ColumnDefinition> listIterator = currTable.getColumnDefinitions().listIterator();
						while(listIterator.hasNext()){
							ColumnDefinition tempCol = listIterator.next();
							System.out.println(tempCol);
							System.out.println(tempCol.getColumnName());
							System.out.println(tempCol.getColDataType());
						}
						String tableName = currTable.getTable().getName();
						tables.put(tableName, currTable);
					}
					else if(stmt instanceof Select){
						SelectBody select = ((Select)stmt).getSelectBody();
						if(select instanceof PlainSelect){
							PlainSelect pselect = (PlainSelect)select;
							System.out.println("Printing complete pselect query: "+pselect);
							System.out.println("Printing get from item: "+pselect.getFromItem());
							System.out.println("Printing get joins: "+pselect.getJoins());
							System.out.println("Where clause is: "+pselect.getWhere());
							System.out.println("Select Items"+pselect.getSelectItems());
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
							JoinOperator finalTableOperator = null;
							if(hasJoin){
								for(Join currJoin:joinDetails){
									FromScanner tempFromScan = new FromScanner(dataDir, tables);
									currJoin.getRightItem().accept(tempFromScan);
									Operator tempTableOperator = tempFromScan.source;
									if(currJoin.isSimple()){
										if(finalTableOperator == null){
											finalTableOperator = new JoinOperator(firstTableOperator, tempTableOperator, null);
										}
										else{
											finalTableOperator = new JoinOperator(finalTableOperator, tempTableOperator, null);
										}
									}
									Expression joinCondition = currJoin.getOnExpression();
									System.out.println("Join condition is: "+joinCondition);
									if(joinCondition != null){
										if(finalTableOperator == null){
											finalTableOperator = new JoinOperator(firstTableOperator, tempTableOperator, joinCondition);
										}
										else{
											finalTableOperator = new JoinOperator(finalTableOperator, tempTableOperator, joinCondition);
										}
										
									}
								}
							}
							SelectionOperator selectOperator = null;
							ProjectionOperator projectOperator = null;
							System.out.println("Has join is: "+hasJoin);
							if(hasJoin){
								selectOperator = new SelectionOperator(finalTableOperator, finalTableOperator.schema, selectCondition);
								projectOperator = new ProjectionOperator(selectOperator, finalTableOperator.schema, selectItems);
							}
							else{
								TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
								List tableList = tablesNamesFinder.getTableList((Select)stmt);
								String tableName = null;
								for (Iterator iter = tableList.iterator(); iter.hasNext();) {
									if(tableName == null){
										tableName = String.valueOf(iter.next());
									}								
									System.out.println("Next table name is: "+tableName);
								}
								CreateTable currTableObject = tables.get(tableName);
								ColumnDefinition[] schema = new ColumnDefinition[currTableObject.getColumnDefinitions().size()]; 
								currTableObject.getColumnDefinitions().toArray(schema);
								selectOperator = new SelectionOperator(firstTableOperator, schema, selectCondition);
								projectOperator = new ProjectionOperator(selectOperator,schema,selectItems);
							}
							Datum[] currTuple = null;
							while((currTuple = projectOperator.readOneTuple()) != null){
								String currDatumString = "";
								for(Datum currDatum:currTuple){
									currDatumString = currDatumString+"|"+currDatum;
								}
								System.out.println(currDatumString.substring(1));
							}
							
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
		//System.out.println(tables);
		//System.out.println(sqlFiles);

	}

}
