/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class ColumnInfo {
	ColumnDefinition colDef;
	String tableName;
	String origTableName;
	int functionType=0;
	ColumnInfo(ColumnDefinition colDef, String tableName, int functionType,String origTableName){
		this.colDef=colDef;
		this.tableName=tableName;
		this.functionType=functionType;
		this.origTableName=origTableName;
	}

}
