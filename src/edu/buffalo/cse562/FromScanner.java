/**
 * 
 */
package edu.buffalo.cse562;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
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
	public ColumnInfo[] schema = null;
	public Operator source = null;
	
	public FromScanner(File basePath, HashMap<String, CreateTable> tables){
		this.basePath=basePath;
		this.tables=tables;
	}

	@Override
	public void visit(Table tableName) {
		if(Main.tpch&&!Main.build)
			return;
		//System.out.println("Visit method with tableName is called");
		CreateTable table = tables.get(tableName.getName().toUpperCase()) == null?tables.get(tableName.getName()):tables.get(tableName.getName().toUpperCase());
		List<?> colDefs = table.getColumnDefinitions();
		ColumnDefinition[] colDefschema = new ColumnDefinition[colDefs.size()];
		schema = new ColumnInfo[colDefs.size()];
		colDefs.toArray(colDefschema);
		for(int i=0;i<colDefschema.length;i++){
			String origTableName = tableName.getName();
			String tableNameEffective=tableName.getAlias() != null?tableName.getAlias():tableName.getName();
			schema[i]=new ColumnInfo(colDefschema[i],tableNameEffective,0,origTableName);
		}
		source = new IndexScanOperator(new File(basePath, tableName.getName()+".dat"), schema);
		
	}
	
	//Helper method to get scan operator to build indexes
	public void visitForBuild(Table tableName) {
		//System.out.println("Visit method with tableName is called");
		CreateTable table = tables.get(tableName.getName().toUpperCase()) == null?tables.get(tableName.getName()):tables.get(tableName.getName().toUpperCase());
		List<?> colDefs = table.getColumnDefinitions();
		ColumnDefinition[] colDefschema = new ColumnDefinition[colDefs.size()];
		schema = new ColumnInfo[4];
		colDefs.toArray(colDefschema);
		for(int i=0;i<colDefschema.length;i++){
			String origTableName = tableName.getName();
			String tableNameEffective=tableName.getAlias() != null?tableName.getAlias():tableName.getName();
			schema[i]=new ColumnInfo(colDefschema[i],tableNameEffective,0,origTableName);
		}
		source = new ScanOperator(new File(basePath, tableName.getName()+".dat"), schema);
		
	}

	@Override
	public void visit(SubSelect subSelect) {
		//System.out.println("Visit method with SubSelect is called");
		SelectBody select = subSelect.getSelectBody();
		if(select instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect)select;
			source = SelectProcessor.processPlainSelect(pselect);
		}		
	}

	@Override
	public void visit(SubJoin arg0) {
		//System.out.println("Visit method with SubJoin is called");
	}

}
