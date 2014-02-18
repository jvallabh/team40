/**
 * 
 */
package edu.buffalo.cse562;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class FromScanner implements FromItemVisitor {
	File basePath;
	HashMap<String, CreateTable> tables;
	public ColumnDefinition[] schema = null;
	public Operator source = null;
	
	public FromScanner(File basePath, HashMap<String, CreateTable> tables){
		this.basePath=basePath;
		this.tables=tables;
	}

	@Override
	public void visit(Table tableName) {
		//System.out.println("Visit method with tableName is called");
		CreateTable table = tables.get(tableName.getName());
		List colDefs = table.getColumnDefinitions();
		schema = new ColumnDefinition[colDefs.size()];
		colDefs.toArray(schema);
		source = new ScanOperator(new File(basePath, tableName.getName()+".dat"), schema);
		
	}

	@Override
	public void visit(SubSelect arg0) {
		//System.out.println("Visit method with SubSelect is called");
		
	}

	@Override
	public void visit(SubJoin arg0) {
		//System.out.println("Visit method with SubJoin is called");
		
	}

}
