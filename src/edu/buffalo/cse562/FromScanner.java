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
	public Column[] schema = null;
	public Operator source = null;
	
	public FromScanner(File basePath, HashMap<String, CreateTable> tables){
		this.basePath=basePath;
		this.tables=tables;
	}

	@Override
	public void visit(Table tableName) {
		CreateTable table = tables.get(tableName.getName());
		List cols = table.getColumnDefinitions();
		schema = new Column[cols.size()];
		for(int i=0;i<cols.size();i++){
			ColumnDefinition col = (ColumnDefinition)cols.get(i);
			schema[i]=new Column(tableName, col.getColumnName());
		}
		source = new ScanOperator(new File(basePath, tableName.getName()+" .dat"));
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}

}
