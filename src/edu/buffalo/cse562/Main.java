package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
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
						System.out.println(currTable.getColumnDefinitions());
						String tableName = currTable.getTable().getName();
						tables.put(tableName, currTable);
					}
					else if(stmt instanceof Select){
						SelectBody select = ((Select)stmt).getSelectBody();
						if(select instanceof PlainSelect){
							PlainSelect pselect = (PlainSelect)select;
							System.out.println("Printing complete pselect query: "+pselect);
							System.out.println("Printing get from item: "+pselect.getFromItem());
							System.out.println("Where clause is: "+pselect.getWhere());
							/*FromScanner fromscan = new FromScanner(dataDir, tables);
							pselect.getFromItem().accept(fromscan);
							Operator oper = fromscan.source;*/
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
