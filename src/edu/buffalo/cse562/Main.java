package edu.buffalo.cse562;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;

import java.util.HashMap;
import java.util.Iterator;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;

import jdbm.RecordManagerFactory;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

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
	public static String swapDir;
	public static ArrayList<File> sqlFiles = new ArrayList<>();
	public static boolean build = false;
	public static ArrayList<Table> tableNames = new ArrayList<>();
	public static String indexDir;
	public static boolean tpch;
	public static RecordManager indexFile;
	public static String howla = new String("select n1.name as suppnation, n2.name as custnation, lineitem.extendedprice * (1 - lineitem.discount) as volume,lineitem.shipdate	from supplier, lineitem, orders, customer, nation n1, nation n2	where supplier.suppkey = lineitem.suppkey and orders.orderkey = lineitem.orderkey and customer.custkey = orders.custkey	and supplier.nationkey = n1.nationkey and customer.nationkey = n2.nationkey	order by lineitem.shipdate");
	public static boolean done;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int i;
		//long millis1 = System.currentTimeMillis() ;
		tables = new HashMap<>();
		for(i=0;i<args.length;i++){
			if(args[i].equals("--data")){
				dataDir = new File(args[i+1]);
				i++;
			}
			else if(args[i].equals("--swap")) {
				swapDir = new String(args[i+1]);
				i++;
			}
			else if(args[i].equals("--build")) {
				build = true;
			}
			else if(args[i].equals("--index")) {
				indexDir = new String(args[i+1]);
				i++;
			}
			else{
				sqlFiles.add(new File(args[i]));
			}
		}
		try{
			Main.indexFile = RecordManagerFactory.createRecordManager(Main.indexDir+"/"+"Index"+"nation");
		}
		catch(Exception e){
			
		}
		for(File sql:sqlFiles){
			if(sql.getName().contains("07"))
				tpch = true;
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
						if(Main.build){
							currTable.getTable().setName(tableName.toLowerCase());
							Main.tableNames.add(currTable.getTable());
						}
					}
					else if(stmt instanceof Select){
						if(Main.build) {
							try{
								FromScanner fromscanner = new FromScanner(Main.dataDir,tables);
								for(Table s: tableNames){
									BuildIndex buildIndex = new BuildIndex(null,0);
									buildIndex.buildTableIndex();
									fromscanner.visit(s);
									for(int col:(int[])buildIndex.tableIndex.get(s.getName())) {
									Operator scanOperator = fromscanner.source;
									buildIndex.input = scanOperator;
									buildIndex.indexFile = ((IndexScanOperator)scanOperator).indexFile;
										buildIndex.buildIndex(col);
									}
									buildIndex.indexFile.close();
								}
								runningJoinCode();
								return;
							}
							catch(Exception e){
								e.printStackTrace();
							}
						}
						else {
							SelectBody select = ((Select)stmt).getSelectBody();
							if(select instanceof PlainSelect){
								PlainSelect pselect = (PlainSelect)select;
								Operator resultOperator = SelectProcessor.processPlainSelect(pselect);
								
								Limit limit = pselect.getLimit();
								boolean hasLimit = limit == null?false:true;
								if(hasLimit){
									Util.printOutputTuples(resultOperator, (int) limit.getRowCount());
								}
								else{
									Util.printOutputTuples(resultOperator);
								}												
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
			tpch=false;
		}
		//long millis2 =  (System.currentTimeMillis() );
		//System.out.println(millis2 - millis1);
		
	}
	
	public static void runningJoinCode(){
		CCJSqlParser parser = null;
		Main.tpch = false;
		try{
			InputStream stream = new ByteArrayInputStream(howla.getBytes(StandardCharsets.UTF_8));
			parser = new CCJSqlParser(stream);
			Statement stmt=parser.Statement();
		SelectBody select = ((Select)stmt).getSelectBody();
		if(select instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect)select;
			Operator resultOperator = SelectProcessor.processPlainSelect(pselect);
			
			Limit limit = pselect.getLimit();
			boolean hasLimit = limit == null?false:true;
				Util.buildIndex(resultOperator);										
		}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
}
